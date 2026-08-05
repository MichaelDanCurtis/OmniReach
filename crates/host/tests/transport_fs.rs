//! Filesystem ops over the websocket.
//!
//! `tests/fs.rs` covers `FsService` directly; this covers the wire contract the
//! client codes against — the op names, the shape of each response, and the
//! binary-frame protocol for transfer.
//!
//! No PTYs are created here, so the `Runtime::drop` teardown trap documented in
//! `tests/replay.rs` does not apply.

use futures_util::{SinkExt, StreamExt};
use omnireach_host::config::Config;
use omnireach_host::sessions::SessionRegistry;
use omnireach_host::transport::{router, AppState};
use omnireach_protocol::{BinaryFrame, FrameKind};
use std::sync::Arc;
use std::time::Duration;
use tokio::net::TcpStream;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

type Ws = WebSocketStream<MaybeTlsStream<TcpStream>>;

async fn start() -> String {
    let config = Arc::new(Config::from_str("").unwrap());
    let registry = Arc::new(SessionRegistry::new(64 * 1024));
    let state = AppState::new(registry, config);
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        let _ = axum::serve(listener, router(state)).await;
    });
    format!("ws://{addr}/ws")
}

/// Send a control request; collect any binary frames that arrive keyed on the
/// same id, then return the control response. Mirrors exactly what the client
/// does — the id is chosen by the caller, so a chunk can never arrive for an
/// unregistered key.
async fn call(
    ws: &mut Ws,
    id: u64,
    op: &str,
    params: serde_json::Value,
) -> (serde_json::Value, Vec<u8>) {
    ws.send(Message::Text(
        serde_json::json!({ "id": id, "op": op, "params": params })
            .to_string()
            .into(),
    ))
    .await
    .unwrap();

    let mut bytes = Vec::new();
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(2), ws.next()).await
        else {
            continue;
        };
        match msg {
            Message::Binary(b) => {
                if let Ok(f) = BinaryFrame::decode(&b) {
                    if f.kind == FrameKind::FsChunk && f.handle as u64 == id {
                        bytes.extend_from_slice(&f.payload);
                    }
                }
            }
            Message::Text(t) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if v["id"] == id {
                    return (v, bytes);
                }
            }
            _ => {}
        }
    }
    panic!("no response to {op}");
}

fn tmpdir(tag: &str) -> std::path::PathBuf {
    let d = std::env::temp_dir().join(format!("omnireach-fs-{tag}-{}", uuid::Uuid::new_v4()));
    std::fs::create_dir_all(&d).unwrap();
    d
}

#[tokio::test]
async fn lists_a_directory_and_reports_its_canonical_path() {
    let url = start().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let dir = tmpdir("list");
    std::fs::write(dir.join("alpha.txt"), b"hello").unwrap();
    std::fs::create_dir(dir.join("sub")).unwrap();

    // A path with `..` in it must come back resolved, not echoed raw —
    // otherwise the UI would show a path that does not name what it displays.
    let messy = dir.join("sub").join("..");
    let (resp, _) = call(
        &mut ws,
        1,
        "fs.list",
        serde_json::json!({ "path": messy.to_string_lossy() }),
    )
    .await;
    assert_eq!(resp["ok"], true, "{resp}");

    let entries = resp["result"]["entries"].as_array().unwrap();
    let names: Vec<&str> = entries
        .iter()
        .map(|e| e["name"].as_str().unwrap())
        .collect();
    assert!(names.contains(&"alpha.txt"), "got {names:?}");
    assert!(names.contains(&"sub"), "got {names:?}");

    let alpha = entries.iter().find(|e| e["name"] == "alpha.txt").unwrap();
    assert_eq!(alpha["size"], 5);
    assert_eq!(alpha["is_dir"], false);
    let sub = entries.iter().find(|e| e["name"] == "sub").unwrap();
    assert_eq!(sub["is_dir"], true);

    assert!(
        !resp["result"]["path"].as_str().unwrap().contains(".."),
        "listing must report the canonicalized path, got {}",
        resp["result"]["path"]
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn reads_a_file_in_chunks_byte_identically() {
    let url = start().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let dir = tmpdir("read");
    let path = dir.join("blob.bin");

    // Deliberately NOT valid UTF-8: the transfer path must be byte-transparent
    // end to end, exactly like PTY output.
    let content: Vec<u8> = (0..5000u32).map(|i| (i % 256) as u8).collect();
    std::fs::write(&path, &content).unwrap();

    let mut got = Vec::new();
    let mut offset = 0u64;
    let mut id = 10;
    loop {
        let (resp, bytes) = call(
            &mut ws,
            id,
            "fs.read",
            serde_json::json!({ "path": path.to_string_lossy(), "offset": offset, "len": 1024 }),
        )
        .await;
        assert_eq!(resp["ok"], true, "{resp}");
        got.extend_from_slice(&bytes);
        let n = resp["result"]["len"].as_u64().unwrap();
        assert_eq!(
            n as usize,
            bytes.len(),
            "declared length must match the bytes actually framed"
        );
        offset += n;
        id += 1;
        if resp["result"]["eof"].as_bool().unwrap() || n == 0 {
            assert_eq!(resp["result"]["total"], content.len() as u64);
            break;
        }
    }
    assert_eq!(
        got, content,
        "chunked read must reassemble byte-identically"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn uploads_in_chunks_and_resumes_at_an_offset() {
    let url = start().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let dir = tmpdir("write");
    let path = dir.join("up.bin");
    let p = path.to_string_lossy().to_string();

    let first: Vec<u8> = (0..800u32).map(|i| (i % 256) as u8).collect();
    let second: Vec<u8> = (800..1600u32).map(|i| (i % 256) as u8).collect();

    // The binary frame is sent BEFORE its control message; websocket ordering
    // makes that deterministic, so the host always has the bytes in hand.
    for (id, offset, chunk) in [(20u64, 0u64, &first), (21, 800, &second)] {
        let frame = BinaryFrame {
            kind: FrameKind::FsChunk,
            handle: id as u32,
            seq: offset,
            payload: chunk.to_vec(),
        };
        ws.send(Message::Binary(frame.encode().into()))
            .await
            .unwrap();
        let (resp, _) = call(
            &mut ws,
            id,
            "fs.write",
            serde_json::json!({ "path": p, "offset": offset, "len": chunk.len() }),
        )
        .await;
        assert_eq!(resp["ok"], true, "{resp}");
        assert_eq!(resp["result"]["written"], chunk.len() as u64);
    }

    let mut expected = first.clone();
    expected.extend_from_slice(&second);
    assert_eq!(std::fs::read(&path).unwrap(), expected);

    // Resume: rewrite the second chunk at its own offset, as a client
    // continuing an interrupted upload would. The result must be unchanged,
    // not appended.
    let frame = BinaryFrame {
        kind: FrameKind::FsChunk,
        handle: 22,
        seq: 800,
        payload: second.clone(),
    };
    ws.send(Message::Binary(frame.encode().into()))
        .await
        .unwrap();
    let (resp, _) = call(
        &mut ws,
        22,
        "fs.write",
        serde_json::json!({ "path": p, "offset": 800, "len": second.len() }),
    )
    .await;
    assert_eq!(resp["ok"], true, "{resp}");
    assert_eq!(
        std::fs::read(&path).unwrap(),
        expected,
        "a resumed write at a known offset must not append"
    );

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn mkdir_rename_and_remove_round_trip() {
    let url = start().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let dir = tmpdir("mutate");

    let made = dir.join("made");
    let (r, _) = call(
        &mut ws,
        30,
        "fs.mkdir",
        serde_json::json!({ "path": made.to_string_lossy() }),
    )
    .await;
    assert_eq!(r["ok"], true, "{r}");
    assert!(made.is_dir());

    let moved = dir.join("moved");
    let (r, _) = call(
        &mut ws,
        31,
        "fs.rename",
        serde_json::json!({ "from": made.to_string_lossy(), "to": moved.to_string_lossy() }),
    )
    .await;
    assert_eq!(r["ok"], true, "{r}");
    assert!(moved.is_dir() && !made.exists());

    let (r, _) = call(
        &mut ws,
        32,
        "fs.remove",
        serde_json::json!({ "path": moved.to_string_lossy() }),
    )
    .await;
    assert_eq!(r["ok"], true, "{r}");
    assert!(!moved.exists());

    std::fs::remove_dir_all(&dir).ok();
}

#[tokio::test]
async fn errors_carry_a_structured_code() {
    let url = start().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    let missing = std::env::temp_dir().join(format!("omnireach-absent-{}", uuid::Uuid::new_v4()));
    let (r, _) = call(
        &mut ws,
        40,
        "fs.list",
        serde_json::json!({ "path": missing.to_string_lossy() }),
    )
    .await;
    assert_eq!(r["ok"], false);
    assert_eq!(
        r["error"]["code"], "NOT_FOUND",
        "the client branches on this code, not on the message: {r}"
    );

    // A missing required param must be rejected, not silently defaulted.
    let (r, _) = call(&mut ws, 41, "fs.list", serde_json::json!({})).await;
    assert_eq!(r["ok"], false);
    assert_eq!(r["error"]["code"], "BAD_PARAMS", "{r}");
}

#[tokio::test]
async fn roots_reports_somewhere_to_start() {
    let url = start().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    let (r, _) = call(&mut ws, 50, "fs.roots", serde_json::json!({})).await;
    assert_eq!(r["ok"], true, "{r}");
    assert!(
        !r["result"]["roots"].as_array().unwrap().is_empty(),
        "the file browser has nowhere to open without at least one root"
    );
}
