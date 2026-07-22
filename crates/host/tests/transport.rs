//! Integration tests for the multiplexed WebSocket transport.
//!
//! # Every test MUST terminate its PTY sessions before returning
//!
//! `Runtime::drop` *waits* for `spawn_blocking` tasks and cannot cancel them.
//! Each `PtySession` parks a blocking task inside a `read()` syscall on the PTY
//! master, which only returns once the last slave fd closes — i.e. once the
//! child dies. A test that leaves a live shell behind therefore hangs the whole
//! test binary forever, with no output and no failure message.
//!
//! `TestHost::shutdown()` is the guard: it kills every session in the registry
//! and waits for each to report dead. Call it at the end of every test that
//! creates a PTY. Tests whose shells exit on their own still call it, because
//! it is cheap and it fails loudly rather than hanging if the assumption breaks.

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

type Client = WebSocketStream<MaybeTlsStream<TcpStream>>;

/// `sh` is the interactive default. The other entries exist so a test can pick
/// a fully-scripted "shell" by name: `PtyCreateParams` carries no argv, so
/// canned argv has to live in the config.
const TEST_CONFIG: &str = r#"
scrollback_bytes = 4194304

[[shells]]
name = "sh"
command = "/bin/sh"
args = []
default = true

[[shells]]
name = "printer"
command = "/bin/sh"
args = ["-c", "printf 'marker-alpha\\n'"]

# ~17 MiB as fast as the pty will take it, then exits. Enough to blow past the
# 4 MiB outbound high-water mark while the test deliberately refuses to read.
[[shells]]
name = "flood"
command = "/bin/sh"
args = ["-c", "yes 0123456789012345678901234567890123456789012345678901234567890123 | head -n 262144"]
"#;

struct TestHost {
    ws_url: String,
    http_url: String,
    registry: Arc<SessionRegistry>,
}

impl TestHost {
    async fn start() -> TestHost {
        let config = Arc::new(Config::from_str(TEST_CONFIG).expect("test config parses"));
        let registry = Arc::new(SessionRegistry::new(config.scrollback_bytes));
        let state = AppState::new(Arc::clone(&registry), config);

        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        tokio::spawn(async move {
            let _ = axum::serve(listener, router(state)).await;
        });

        TestHost {
            ws_url: format!("ws://{addr}/ws"),
            http_url: format!("http://{addr}"),
            registry,
        }
    }

    async fn connect(&self) -> Client {
        tokio_tungstenite::connect_async(&self.ws_url)
            .await
            .expect("websocket upgrade")
            .0
    }

    /// Kill every session and wait for the blocking read loops to unwind.
    /// See the module docs: skipping this hangs the test binary.
    async fn shutdown(&self) {
        for info in self.registry.list() {
            let _ = self.registry.kill(info.session_id);
        }
        for _ in 0..300 {
            if self.registry.list().iter().all(|i| !i.alive) {
                return;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        panic!("sessions still alive at teardown — the test binary would hang");
    }
}

/// Sends a control request and returns the response with the matching `id`,
/// accumulating any PTY bytes seen along the way.
async fn call(ws: &mut Client, id: u64, op: &str, params: serde_json::Value) -> serde_json::Value {
    let req = serde_json::json!({ "id": id, "op": op, "params": params });
    ws.send(Message::Text(req.to_string().into()))
        .await
        .unwrap();
    await_response(ws, id).await
}

async fn await_response(ws: &mut Client, id: u64) -> serde_json::Value {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(15);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        else {
            continue;
        };
        if let Message::Text(t) = msg {
            let v: serde_json::Value = serde_json::from_str(&t).expect("control frames are JSON");
            // Every text frame is either a response (has `id`) or an event
            // (has `event`). Nothing else is allowed on the text channel.
            assert!(
                v.get("id").is_some() || v.get("event").is_some(),
                "unexpected text frame: {v}"
            );
            if v["id"] == id {
                return v;
            }
        }
    }
    panic!("no response for request id {id}");
}

/// Bytes read for one handle, plus the resume point: `seq + len` of the last
/// frame consumed, i.e. the first sequence number the client has *not* seen.
type Drained = (Vec<u8>, u64);

/// Reads until `want` appears in the concatenated PTY payload for `handle`,
/// or until `timeout`. `Err` carries whatever had arrived by then.
async fn collect_until(
    ws: &mut Client,
    handle: u32,
    want: &[u8],
    timeout: Duration,
) -> Result<Drained, Drained> {
    let mut seen: Vec<u8> = Vec::new();
    let mut next_seq = 0u64;
    let deadline = tokio::time::Instant::now() + timeout;
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(2), ws.next()).await
        else {
            continue;
        };
        match msg {
            Message::Binary(b) => {
                let f = BinaryFrame::decode(&b).expect("host emits well-formed frames");
                assert_eq!(
                    f.kind,
                    FrameKind::PtyOutput,
                    "host must only send PtyOutput frames"
                );
                if f.handle != handle {
                    continue;
                }
                seen.extend_from_slice(&f.payload);
                next_seq = f.seq + f.payload.len() as u64;
                if seen.windows(want.len()).any(|w| w == want) {
                    return Ok((seen, next_seq));
                }
            }
            Message::Text(t) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                // The one rule that matters: PTY bytes never ride inside JSON.
                assert!(
                    v.get("payload").is_none() && v.get("data").is_none(),
                    "pty bytes must not be base64'd into a control frame: {v}"
                );
            }
            _ => {}
        }
    }
    Err((seen, next_seq))
}

fn expect_ok(v: &serde_json::Value) -> &serde_json::Value {
    assert_eq!(v["ok"], true, "expected ok response, got {v}");
    &v["result"]
}

#[tokio::test]
async fn health_endpoint_responds() {
    let host = TestHost::start().await;
    let body = reqwest::get(format!("{}/health", host.http_url))
        .await
        .unwrap()
        .text()
        .await
        .unwrap();
    assert!(body.contains("ok"), "got {body}");
}

#[tokio::test]
async fn creates_a_pty_and_streams_its_output_as_binary() {
    let host = TestHost::start().await;
    let mut ws = host.connect().await;

    let resp = call(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": "sh", "cwd": null, "cols": 80, "rows": 24}),
    )
    .await;
    let result = expect_ok(&resp);
    let handle = result["handle"].as_u64().unwrap() as u32;
    assert!(result["session_id"].is_string(), "create returns an id");

    // The quoting is deliberate: the terminal echoes the literal keystrokes
    // (`ws-round''trip`), so finding the joined marker proves the shell really
    // executed the command rather than proving the echo works.
    let input = BinaryFrame {
        kind: FrameKind::PtyInput,
        handle,
        seq: 0,
        payload: b"printf 'ws-round''trip\\n'\n".to_vec(),
    };
    ws.send(Message::Binary(input.encode().into()))
        .await
        .unwrap();

    let (seen, _) = collect_until(&mut ws, handle, b"ws-roundtrip", Duration::from_secs(15))
        .await
        .unwrap_or_else(|(got, _)| {
            panic!(
                "never saw echoed output; got {:?}",
                String::from_utf8_lossy(&got)
            )
        });
    assert!(!seen.is_empty());

    host.shutdown().await;
}

#[tokio::test]
async fn pty_bytes_survive_the_round_trip_undecoded() {
    let host = TestHost::start().await;
    let mut ws = host.connect().await;

    let resp = call(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": "sh", "cols": 80, "rows": 24}),
    )
    .await;
    let handle = expect_ok(&resp)["handle"].as_u64().unwrap() as u32;

    // 0xFF is not valid UTF-8 anywhere. If anything on the host path decoded
    // the stream it would arrive as U+FFFD (0xEF 0xBF 0xBD) instead.
    let input = BinaryFrame {
        kind: FrameKind::PtyInput,
        handle,
        seq: 0,
        payload: b"printf 'A\\377B\\n'\n".to_vec(),
    };
    ws.send(Message::Binary(input.encode().into()))
        .await
        .unwrap();

    let (seen, _) = collect_until(
        &mut ws,
        handle,
        &[b'A', 0xFF, b'B'],
        Duration::from_secs(15),
    )
    .await
    .unwrap_or_else(|(got, _)| panic!("raw byte 0xFF did not survive; got {got:?}"));
    assert!(
        !seen.windows(3).any(|w| w == [0xEF, 0xBF, 0xBD]),
        "output contains U+FFFD — something decoded the pty stream"
    );

    host.shutdown().await;
}

#[tokio::test]
async fn a_new_connection_can_attach_and_replay_scrollback() {
    let host = TestHost::start().await;

    let session_id = {
        let mut ws = host.connect().await;
        let resp = call(
            &mut ws,
            1,
            "pty.create",
            serde_json::json!({"shell": "printer", "cols": 80, "rows": 24}),
        )
        .await;
        let result = expect_ok(&resp);
        let handle = result["handle"].as_u64().unwrap() as u32;
        let id = result["session_id"].as_str().unwrap().to_string();
        collect_until(&mut ws, handle, b"marker-alpha", Duration::from_secs(15))
            .await
            .expect("first connection should see the marker");
        id
        // ws is dropped here: the socket goes away, the session must not.
    };

    let mut ws2 = host.connect().await;
    let resp = call(
        &mut ws2,
        7,
        "pty.attach",
        serde_json::json!({"session_id": session_id, "since": 0}),
    )
    .await;
    let result = expect_ok(&resp);
    let handle = result["handle"].as_u64().unwrap() as u32;
    assert_eq!(result["cols"], 80);
    assert_eq!(result["rows"], 24);
    assert!(
        result["truncated_from"].is_null(),
        "the ring is far larger than this output, nothing should be truncated"
    );

    let (replayed, _) = collect_until(&mut ws2, handle, b"marker-alpha", Duration::from_secs(15))
        .await
        .expect("reattach must replay the scrollback");
    assert!(!replayed.is_empty());

    host.shutdown().await;
}

#[tokio::test]
async fn attaching_with_a_since_cursor_skips_delivered_bytes() {
    let host = TestHost::start().await;
    let mut ws = host.connect().await;

    let resp = call(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": "printer", "cols": 80, "rows": 24}),
    )
    .await;
    let result = expect_ok(&resp);
    let first_handle = result["handle"].as_u64().unwrap() as u32;
    let session_id = result["session_id"].as_str().unwrap().to_string();
    let (_, since) = collect_until(
        &mut ws,
        first_handle,
        b"marker-alpha",
        Duration::from_secs(15),
    )
    .await
    .expect("marker");

    let resp = call(
        &mut ws,
        2,
        "pty.attach",
        serde_json::json!({"session_id": session_id, "since": since}),
    )
    .await;
    let handle = expect_ok(&resp)["handle"].as_u64().unwrap() as u32;
    assert_ne!(handle, first_handle, "each attach gets its own handle");

    // Anything that does arrive for the new handle must start at or after the
    // cursor — the backlog below `since` must not be replayed again.
    let deadline = tokio::time::Instant::now() + Duration::from_millis(750);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(Message::Binary(b)))) =
            tokio::time::timeout(Duration::from_millis(250), ws.next()).await
        else {
            continue;
        };
        let f = BinaryFrame::decode(&b).unwrap();
        if f.handle == handle {
            assert!(
                f.seq >= since,
                "replayed already-delivered bytes: seq {} < since {since}",
                f.seq
            );
        }
    }

    host.shutdown().await;
}

#[tokio::test]
async fn lists_resizes_and_kills_sessions() {
    let host = TestHost::start().await;
    let mut ws = host.connect().await;

    let resp = call(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": "sh", "cols": 80, "rows": 24}),
    )
    .await;
    let result = expect_ok(&resp);
    let handle = result["handle"].as_u64().unwrap() as u32;
    let session_id = result["session_id"].as_str().unwrap().to_string();

    let resp = call(&mut ws, 2, "pty.list", serde_json::json!({})).await;
    let sessions = expect_ok(&resp)["sessions"].as_array().unwrap().clone();
    assert!(
        sessions
            .iter()
            .any(|s| s["session_id"].as_str() == Some(session_id.as_str())),
        "pty.list must report the new session: {sessions:?}"
    );

    let resp = call(
        &mut ws,
        3,
        "pty.resize",
        serde_json::json!({"handle": handle, "cols": 120, "rows": 40}),
    )
    .await;
    expect_ok(&resp);
    let id: uuid::Uuid = session_id.parse().unwrap();
    assert_eq!(host.registry.get(id).unwrap().size(), (120, 40));

    let resp = call(
        &mut ws,
        4,
        "pty.detach",
        serde_json::json!({"handle": handle}),
    )
    .await;
    expect_ok(&resp);

    let resp = call(
        &mut ws,
        5,
        "pty.kill",
        serde_json::json!({"session_id": session_id}),
    )
    .await;
    expect_ok(&resp);

    host.shutdown().await;
}

#[tokio::test]
async fn bad_requests_produce_structured_errors() {
    let host = TestHost::start().await;
    let mut ws = host.connect().await;

    let v = call(&mut ws, 1, "pty.nonsense", serde_json::json!({})).await;
    assert_eq!(v["ok"], false);
    assert_eq!(v["error"]["code"], "UNKNOWN_OP");

    let v = call(
        &mut ws,
        2,
        "pty.create",
        serde_json::json!({"shell": "not-configured", "cols": 80, "rows": 24}),
    )
    .await;
    assert_eq!(v["error"]["code"], "SHELL_NOT_FOUND");

    let v = call(&mut ws, 3, "pty.create", serde_json::json!({"cols": 80})).await;
    assert_eq!(v["error"]["code"], "BAD_PARAMS");

    let v = call(
        &mut ws,
        4,
        "pty.attach",
        serde_json::json!({"session_id": uuid::Uuid::new_v4(), "since": 0}),
    )
    .await;
    assert_eq!(v["error"]["code"], "NO_SUCH_SESSION");

    let v = call(
        &mut ws,
        5,
        "pty.resize",
        serde_json::json!({"handle": 4242, "cols": 10, "rows": 10}),
    )
    .await;
    assert_eq!(v["error"]["code"], "NO_SUCH_HANDLE");

    // A malformed text frame must be ignored, not kill the connection.
    ws.send(Message::Text("{not json".to_string().into()))
        .await
        .unwrap();
    let v = call(&mut ws, 6, "pty.list", serde_json::json!({})).await;
    expect_ok(&v);

    host.shutdown().await;
}

#[tokio::test]
async fn client_is_told_when_the_shell_exits() {
    let host = TestHost::start().await;
    let mut ws = host.connect().await;

    let resp = call(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": "printer", "cols": 80, "rows": 24}),
    )
    .await;
    let session_id = expect_ok(&resp)["session_id"].as_str().unwrap().to_string();

    let deadline = tokio::time::Instant::now() + Duration::from_secs(20);
    let mut saw_exit = false;
    while tokio::time::Instant::now() < deadline && !saw_exit {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(2), ws.next()).await
        else {
            continue;
        };
        if let Message::Text(t) = msg {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            if v["event"] == "pty.exit" {
                assert_eq!(v["session_id"], session_id);
                saw_exit = true;
            }
        }
    }
    assert!(saw_exit, "expected a pty.exit event after the shell exited");

    host.shutdown().await;
}

/// The load-bearing backpressure test.
///
/// The client connects, starts a shell that emits ~17 MiB as fast as the pty
/// allows, then *stops reading the socket*. The kernel buffers fill, the writer
/// task parks inside `send`, and the per-attachment outbound accounting climbs
/// past `SEND_QUEUE_HIGH_WATER`. The host must respond by discarding what it
/// has queued for that session, announcing `pty.resync`, and resuming from the
/// ring tail — not by growing a queue until it runs out of memory.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn backpressure_drops_queued_output_and_resyncs() {
    let host = TestHost::start().await;
    let mut ws = host.connect().await;

    let resp = call(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": "flood", "cols": 200, "rows": 50}),
    )
    .await;
    let result = expect_ok(&resp);
    let handle = result["handle"].as_u64().unwrap() as u32;
    let session_id = result["session_id"].as_str().unwrap().to_string();

    // Deliberately do not poll the socket: this is what a stalled client looks
    // like from the host's point of view.
    tokio::time::sleep(Duration::from_secs(3)).await;

    let mut resync_from: Option<u64> = None;
    let mut bytes_before_resync: u64 = 0;
    let mut bytes_after_resync: u64 = 0;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(45);
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_secs(5), ws.next()).await
        else {
            break;
        };
        match msg {
            Message::Text(t) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if v["event"] == "pty.resync" && resync_from.is_none() {
                    assert_eq!(v["session_id"], session_id);
                    resync_from = Some(v["from_seq"].as_u64().expect("from_seq is a number"));
                }
            }
            Message::Binary(b) => {
                let f = BinaryFrame::decode(&b).unwrap();
                if f.handle != handle {
                    continue;
                }
                match resync_from {
                    None => bytes_before_resync += f.payload.len() as u64,
                    Some(from) => {
                        assert!(
                            f.seq >= from,
                            "post-resync frame seq {} is behind from_seq {from}",
                            f.seq
                        );
                        bytes_after_resync += f.payload.len() as u64;
                        if bytes_after_resync > 64 * 1024 {
                            break;
                        }
                    }
                }
            }
            _ => {}
        }
    }

    let from = resync_from.expect("the host must emit pty.resync when the client falls behind");
    assert!(
        from > 0,
        "a resync must skip forward into the stream, not restart it"
    );
    // Forward-only. `>` rather than `>=` would be wrong: if the writer happened
    // to drain the queue before the epoch bump reached it, every frame really
    // was delivered and there is no gap to skip — a legal outcome. What must
    // never happen is the host rewinding and re-delivering bytes the client
    // already has.
    assert!(
        from >= bytes_before_resync,
        "resync rewound: from_seq {from} is behind the {bytes_before_resync} \
         bytes the client had already received"
    );
    assert!(
        bytes_after_resync > 0,
        "the host must keep streaming after a resync"
    );

    host.shutdown().await;
}

/// Types a command into an attached pty, as a binary `PtyInput` frame.
async fn type_line(ws: &mut Client, handle: u32, s: &str) {
    let frame = BinaryFrame {
        kind: FrameKind::PtyInput,
        handle,
        seq: 0,
        payload: s.as_bytes().to_vec(),
    };
    ws.send(Message::Binary(frame.encode().into()))
        .await
        .unwrap();
}

/// Counts complete `line-<n>` records. The pty's ONLCR translation turns the
/// shell's `\n` into `\r\n`, so either terminator is accepted — matching on the
/// bare prefix would count `line-1` inside `line-19`.
fn count_line(hay: &[u8], n: u32) -> usize {
    let needle = format!("line-{n}");
    let needle = needle.as_bytes();
    let mut hits = 0;
    for (i, w) in hay.windows(needle.len()).enumerate() {
        if w == needle && matches!(hay.get(i + needle.len()), Some(b'\r') | Some(b'\n')) {
            hits += 1;
        }
    }
    hits
}

/// Disconnecting mid-stream and reattaching at the last sequence number seen
/// must produce a stream byte-identical to one that never dropped: no line
/// repeated, none missing.
///
/// A duplicate means the resume offset was too early, a gap means it was too
/// late. Both are silent corruption of a terminal transcript, which is the one
/// failure this transport cannot have.
#[tokio::test]
async fn reconnecting_at_the_last_seq_neither_duplicates_nor_drops_output() {
    let host = TestHost::start().await;

    // 200 columns so the shell's echo of these commands never soft-wraps.
    let mut ws = host.connect().await;
    let resp = call(
        &mut ws,
        1,
        "pty.create",
        serde_json::json!({"shell": "sh", "cols": 200, "rows": 24}),
    )
    .await;
    let result = expect_ok(&resp);
    let handle = result["handle"].as_u64().unwrap() as u32;
    let session_id = result["session_id"].as_str().unwrap().to_string();

    type_line(
        &mut ws,
        handle,
        "i=0; while [ $i -lt 60 ]; do printf 'line-%d\\n' $i; i=$((i+1)); done\n",
    )
    .await;
    let (first_half, last_seq) =
        collect_until(&mut ws, handle, b"line-59", Duration::from_secs(20))
            .await
            .expect("first half of the stream");
    assert!(last_seq > 0);

    // Queue the rest, then walk away before any of it exists.
    type_line(
        &mut ws,
        handle,
        "sleep 1; i=60; while [ $i -lt 120 ]; do printf 'line-%d\\n' $i; i=$((i+1)); done\n",
    )
    .await;
    tokio::time::sleep(Duration::from_millis(300)).await;
    drop(ws);

    tokio::time::sleep(Duration::from_millis(2500)).await;

    let mut ws2 = host.connect().await;
    let resp = call(
        &mut ws2,
        2,
        "pty.attach",
        serde_json::json!({"session_id": session_id, "since": last_seq}),
    )
    .await;
    let result = expect_ok(&resp);
    let handle2 = result["handle"].as_u64().unwrap() as u32;
    assert!(
        result["truncated_from"].is_null(),
        "a 4 MiB ring must not truncate this test's output; got {result}"
    );

    let second_half = collect_until(&mut ws2, handle2, b"line-119", Duration::from_secs(20))
        .await
        .map(|(bytes, _)| bytes)
        .unwrap_or_else(|(bytes, _)| bytes);

    // Kill before asserting: a panic here would otherwise leave a live shell
    // and hang the whole test binary instead of reporting the failure.
    host.shutdown().await;

    let mut combined = first_half;
    combined.extend_from_slice(&second_half);
    for i in 0..120u32 {
        assert_eq!(
            count_line(&combined, i),
            1,
            "line-{i} appeared {} times across the disconnect",
            count_line(&combined, i)
        );
    }
}
