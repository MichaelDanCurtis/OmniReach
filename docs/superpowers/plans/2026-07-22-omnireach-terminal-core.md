# OmniReach Terminal Core Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Build a Windows host and macOS client that expose persistent, reconnectable PTY sessions over a Cloudflare Access–protected tunnel.

**Architecture:** A Rust host owns PTY sessions independently of any network connection, buffering each session's output in a bounded byte ring with monotonic sequence numbers. A single multiplexed WebSocket carries JSON control frames and binary bulk data. A Tauri v2 macOS client attaches to sessions by `(session_id, since_seq)` and replays missed output on reconnect.

**Tech Stack:** Rust 1.96 · axum 0.8 · tokio 1.53 · portable-pty 0.9 · jsonwebtoken 10.4 · tokio-tungstenite 0.30 · Tauri v2 · xterm.js

---

## Global Constraints

These apply to **every** task. Verified against the actual crate sources on 2026-07-22 — do not substitute versions or APIs from memory.

- **Rust edition 2021**, toolchain 1.96.0.
- **Exact dependency versions:** `axum = "0.8.9"`, `tokio = "1.53"`, `portable-pty = "0.9.0"`, `jsonwebtoken = "10.4"`, `tokio-tungstenite = "0.30"`, `serde = "1.0"`, `serde_json = "1.0"`, `uuid = "1.24"`, `thiserror = "2.0"`, `tracing = "0.1"`, `futures-util = "0.3"`.
- **`jsonwebtoken` MUST enable a crypto backend:** `jsonwebtoken = { version = "10.4", features = ["aws_lc_rs"] }`. The default features are `use_pem` only, which provides *no* verifier. Without this, `decode` compiles cleanly and **panics at runtime** with "Could not automatically determine the process-level CryptoProvider". This is the single most likely wasted hour in the project.
- **axum 0.8 path syntax is `/{param}`, not `/:param`.** The `:param` form from axum 0.7 and nearly every tutorial online will panic at router construction.
- **Never decode PTY bytes to a `String` on the host.** No `String::from_utf8_lossy`, no `str::from_utf8`, anywhere in the PTY read, ring, or send path. Multi-byte sequences split across reads corrupt permanently once decoded. Bytes in, bytes out; xterm.js reassembles.
- **All host code must pass `cargo check --target x86_64-pc-windows-msvc`.** Development and unit tests run on macOS against Unix PTYs; the Windows target is type-checked only. Run it before every commit.
- **TDD:** every task writes a failing test first, verifies the failure, then implements.
- Commit after every task with a `feat:` / `test:` / `chore:` prefix.

### Known verification gap

No Windows machine is available in this environment. ConPTY behavior, `pwsh.exe` spawning, and the produced `.exe` are **type-checked but never executed**. Every task that touches Windows-specific behavior says so explicitly and states what a human must verify on real hardware. Do not report these as tested.

---

## File Structure

```
Cargo.toml                        workspace root
crates/
  protocol/
    src/lib.rs                    re-exports
    src/frame.rs                  binary frame encode/decode
    src/control.rs                JSON control request/response/event types
  host/
    src/main.rs                   entrypoint, config load, server start
    src/config.rs                 TOML config + defaults
    src/ring.rs                   bounded byte ring with seq numbers
    src/pty.rs                    PtySession: child + ring + broadcast
    src/sessions.rs               SessionRegistry
    src/auth.rs                   JWKS cache + Access JWT validation
    src/transport.rs              axum WS upgrade, multiplex, backpressure
    tests/replay.rs               THE load-bearing reconnect test
client/                           Tauri v2 app (Tasks 11-14)
scripts/check.sh                  fmt + clippy + test + windows check
```

---

### Task 1: Workspace scaffold and check script

**Files:**
- Create: `Cargo.toml`, `crates/protocol/Cargo.toml`, `crates/protocol/src/lib.rs`, `crates/host/Cargo.toml`, `crates/host/src/main.rs`, `scripts/check.sh`, `.gitignore`, `rust-toolchain.toml`

**Interfaces:**
- Produces: a workspace where `cargo test` and `cargo check --target x86_64-pc-windows-msvc` both succeed.

- [ ] **Step 1: Create the workspace root `Cargo.toml`**

```toml
[workspace]
resolver = "2"
members = ["crates/protocol", "crates/host"]

[workspace.package]
edition = "2021"
rust-version = "1.96"

[workspace.dependencies]
serde = { version = "1.0", features = ["derive"] }
serde_json = "1.0"
thiserror = "2.0"
tracing = "0.1"
uuid = { version = "1.24", features = ["v4", "serde"] }
tokio = { version = "1.53", features = ["full"] }
```

- [ ] **Step 2: Create `rust-toolchain.toml`**

```toml
[toolchain]
channel = "1.96.0"
targets = ["x86_64-pc-windows-msvc"]
```

- [ ] **Step 3: Create `crates/protocol/Cargo.toml`**

```toml
[package]
name = "omnireach-protocol"
version = "0.1.0"
edition.workspace = true

[dependencies]
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
uuid.workspace = true
```

- [ ] **Step 4: Create `crates/protocol/src/lib.rs`**

```rust
pub mod control;
pub mod frame;

pub use control::{ControlEvent, ControlRequest, ControlResponse, ProtocolError};
pub use frame::{BinaryFrame, FrameKind, HEADER_LEN};
```

Create empty `crates/protocol/src/control.rs` and `crates/protocol/src/frame.rs` for now — Tasks 2 and 3 fill them.

- [ ] **Step 5: Create `crates/host/Cargo.toml`**

```toml
[package]
name = "omnireach-host"
version = "0.1.0"
edition.workspace = true

[dependencies]
omnireach-protocol = { path = "../protocol" }
axum = { version = "0.8.9", features = ["ws"] }
tokio.workspace = true
serde.workspace = true
serde_json.workspace = true
thiserror.workspace = true
tracing = "0.1"
tracing-subscriber = { version = "0.3", features = ["env-filter"] }
uuid.workspace = true
portable-pty = "0.9.0"
jsonwebtoken = { version = "10.4", features = ["aws_lc_rs"] }
reqwest = { version = "0.12", features = ["json", "rustls-tls"], default-features = false }
futures-util = "0.3"
toml = "0.8"
dirs = "5"
parking_lot = "0.12"

[dev-dependencies]
tokio-tungstenite = "0.30"
```

- [ ] **Step 6: Create `crates/host/src/main.rs` placeholder**

```rust
fn main() {
    println!("omnireach-host");
}
```

- [ ] **Step 7: Create `scripts/check.sh`**

```bash
#!/usr/bin/env bash
set -euo pipefail
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all
echo "--- windows target type-check ---"
cargo check --all --target x86_64-pc-windows-msvc
```

Make it executable: `chmod +x scripts/check.sh`

- [ ] **Step 8: Create `.gitignore`**

```
/target
/client/node_modules
/client/dist
*.log
```

- [ ] **Step 9: Verify the whole thing builds for both targets**

Run: `cargo build --all && cargo check --all --target x86_64-pc-windows-msvc`
Expected: both succeed with no errors.

- [ ] **Step 10: Commit**

```bash
git add -A
git commit -m "chore: cargo workspace scaffold with windows cross-check"
```

---

### Task 2: Ring buffer

The most important data structure in the system. Everything about session persistence rests on it.

**Files:**
- Create: `crates/host/src/ring.rs`
- Modify: `crates/host/src/main.rs` (add `mod ring;`)

**Interfaces:**
- Produces:
  - `Ring::new(capacity: usize) -> Ring`
  - `Ring::write(&mut self, data: &[u8])`
  - `Ring::head_seq(&self) -> u64`
  - `Ring::oldest_seq(&self) -> u64`
  - `Ring::read_since(&self, seq: u64) -> (u64, Vec<u8>)` — returns `(actual_start_seq, bytes)`

- [ ] **Step 1: Write the failing tests**

Create `crates/host/src/ring.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn empty_ring_reports_zero_seqs() {
        let r = Ring::new(16);
        assert_eq!(r.head_seq(), 0);
        assert_eq!(r.oldest_seq(), 0);
        assert_eq!(r.read_since(0), (0, vec![]));
    }

    #[test]
    fn reads_back_everything_written_when_under_capacity() {
        let mut r = Ring::new(16);
        r.write(b"hello");
        assert_eq!(r.head_seq(), 5);
        assert_eq!(r.oldest_seq(), 0);
        assert_eq!(r.read_since(0), (0, b"hello".to_vec()));
    }

    #[test]
    fn reads_suffix_from_midpoint() {
        let mut r = Ring::new(16);
        r.write(b"hello");
        assert_eq!(r.read_since(2), (2, b"llo".to_vec()));
    }

    #[test]
    fn overwrites_oldest_when_full_and_reports_truncation() {
        let mut r = Ring::new(4);
        r.write(b"abcdef");
        assert_eq!(r.head_seq(), 6);
        assert_eq!(r.oldest_seq(), 2);
        // asking for seq 0 gets clamped up to oldest_seq
        assert_eq!(r.read_since(0), (2, b"cdef".to_vec()));
    }

    #[test]
    fn single_write_larger_than_capacity_keeps_only_the_tail() {
        let mut r = Ring::new(4);
        r.write(b"abcdefghij");
        assert_eq!(r.head_seq(), 10);
        assert_eq!(r.oldest_seq(), 6);
        assert_eq!(r.read_since(0), (6, b"ghij".to_vec()));
    }

    #[test]
    fn wraps_correctly_across_many_writes() {
        let mut r = Ring::new(8);
        for _ in 0..10 {
            r.write(b"xyz");
        }
        assert_eq!(r.head_seq(), 30);
        assert_eq!(r.oldest_seq(), 22);
        let (start, bytes) = r.read_since(22);
        assert_eq!(start, 22);
        assert_eq!(bytes.len(), 8);
        // "xyzxyzxyz..." — byte at absolute index n is "xyz"[n % 3]
        let expected: Vec<u8> = (22u64..30).map(|n| b"xyz"[(n % 3) as usize]).collect();
        assert_eq!(bytes, expected);
    }

    #[test]
    fn read_since_future_seq_returns_empty() {
        let mut r = Ring::new(16);
        r.write(b"hello");
        assert_eq!(r.read_since(99), (5, vec![]));
    }

    #[test]
    fn property_read_since_always_yields_a_suffix() {
        let mut r = Ring::new(64);
        let mut all: Vec<u8> = Vec::new();
        let mut n: u8 = 0;
        for len in [1usize, 7, 3, 31, 64, 2, 100, 5] {
            let chunk: Vec<u8> = (0..len)
                .map(|_| {
                    n = n.wrapping_add(1);
                    n
                })
                .collect();
            r.write(&chunk);
            all.extend_from_slice(&chunk);

            let (start, got) = r.read_since(r.oldest_seq());
            let expected = &all[start as usize..];
            assert_eq!(got, expected, "failed after writing {len} bytes");
        }
    }

    #[test]
    fn multibyte_utf8_split_across_writes_survives_replay() {
        // "日本語" is 9 bytes; write it split mid-character.
        let s = "日本語".as_bytes();
        let mut r = Ring::new(64);
        r.write(&s[..4]); // splits the 2nd codepoint
        r.write(&s[4..]);
        let (_, got) = r.read_since(0);
        assert_eq!(got, s, "ring must be byte-transparent, never decoding");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p omnireach-host ring`
Expected: FAIL — `cannot find type 'Ring' in this scope`

- [ ] **Step 3: Implement the ring**

Prepend to `crates/host/src/ring.rs`:

```rust
/// A bounded byte ring that tracks a monotonic sequence number.
///
/// `head_seq` counts every byte ever written, whether or not it is still
/// resident. Bytes older than `oldest_seq()` have been overwritten.
///
/// This type is deliberately byte-transparent: it never interprets, decodes,
/// or validates its contents. PTY output is a stream of VT escape sequences
/// and arbitrary UTF-8, and decoding it here would corrupt multi-byte
/// characters that straddle a write boundary.
pub struct Ring {
    buf: Box<[u8]>,
    head_seq: u64,
}

impl Ring {
    pub fn new(capacity: usize) -> Self {
        Self {
            buf: vec![0u8; capacity].into_boxed_slice(),
            head_seq: 0,
        }
    }

    pub fn head_seq(&self) -> u64 {
        self.head_seq
    }

    pub fn oldest_seq(&self) -> u64 {
        let cap = self.buf.len() as u64;
        self.head_seq.saturating_sub(cap.min(self.head_seq))
    }

    pub fn write(&mut self, data: &[u8]) {
        let cap = self.buf.len();
        let total = data.len() as u64;
        if cap == 0 {
            self.head_seq += total;
            return;
        }

        // A single write larger than capacity: only its tail can survive.
        let tail = if data.len() > cap {
            &data[data.len() - cap..]
        } else {
            data
        };
        let skipped = (data.len() - tail.len()) as u64;

        let pos = ((self.head_seq + skipped) % cap as u64) as usize;
        let first = tail.len().min(cap - pos);
        self.buf[pos..pos + first].copy_from_slice(&tail[..first]);
        if first < tail.len() {
            let rest = tail.len() - first;
            self.buf[..rest].copy_from_slice(&tail[first..]);
        }

        self.head_seq += total;
    }

    /// Returns `(actual_start_seq, bytes)`. If `seq` is older than
    /// `oldest_seq()` the start is clamped forward and the caller must tell
    /// the client its stream was truncated.
    pub fn read_since(&self, seq: u64) -> (u64, Vec<u8>) {
        let cap = self.buf.len();
        let start = seq.max(self.oldest_seq()).min(self.head_seq);
        let n = (self.head_seq - start) as usize;
        if n == 0 || cap == 0 {
            return (start, Vec::new());
        }

        let pos = (start % cap as u64) as usize;
        let mut out = Vec::with_capacity(n);
        let first = n.min(cap - pos);
        out.extend_from_slice(&self.buf[pos..pos + first]);
        if first < n {
            out.extend_from_slice(&self.buf[..n - first]);
        }
        (start, out)
    }
}
```

Add `mod ring;` to `crates/host/src/main.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-host ring`
Expected: PASS, 9 tests.

- [ ] **Step 5: Verify Windows target still checks**

Run: `cargo check --all --target x86_64-pc-windows-msvc`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add crates/host/src/ring.rs crates/host/src/main.rs
git commit -m "feat: bounded byte ring with monotonic sequence numbers"
```

---

### Task 3: Binary frame codec

**Files:**
- Create/replace: `crates/protocol/src/frame.rs`

**Interfaces:**
- Consumes: nothing.
- Produces:
  - `FrameKind` enum: `PtyOutput = 0x01`, `PtyInput = 0x02`, `FsChunk = 0x03`
  - `HEADER_LEN: usize = 13`
  - `BinaryFrame { kind: FrameKind, handle: u32, seq: u64, payload: Vec<u8> }`
  - `BinaryFrame::encode(&self) -> Vec<u8>`
  - `BinaryFrame::decode(bytes: &[u8]) -> Result<BinaryFrame, FrameError>`

Layout, little-endian: `[0] kind u8 | [1..5] handle u32 | [5..13] seq u64 | [13..] payload`.

- [ ] **Step 1: Write the failing tests**

Create `crates/protocol/src/frame.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_a_frame() {
        let f = BinaryFrame {
            kind: FrameKind::PtyOutput,
            handle: 7,
            seq: 123_456_789,
            payload: b"hello world".to_vec(),
        };
        let bytes = f.encode();
        assert_eq!(bytes.len(), HEADER_LEN + 11);
        let back = BinaryFrame::decode(&bytes).expect("decode");
        assert_eq!(back.kind, FrameKind::PtyOutput);
        assert_eq!(back.handle, 7);
        assert_eq!(back.seq, 123_456_789);
        assert_eq!(back.payload, b"hello world");
    }

    #[test]
    fn round_trips_boundary_values() {
        let f = BinaryFrame {
            kind: FrameKind::PtyInput,
            handle: u32::MAX,
            seq: u64::MAX,
            payload: vec![],
        };
        let back = BinaryFrame::decode(&f.encode()).expect("decode");
        assert_eq!(back.handle, u32::MAX);
        assert_eq!(back.seq, u64::MAX);
        assert!(back.payload.is_empty());
    }

    #[test]
    fn round_trips_arbitrary_bytes_including_invalid_utf8() {
        let payload = vec![0xff, 0xfe, 0x00, 0x80, 0xc3];
        let f = BinaryFrame {
            kind: FrameKind::PtyOutput,
            handle: 1,
            seq: 0,
            payload: payload.clone(),
        };
        assert_eq!(BinaryFrame::decode(&f.encode()).unwrap().payload, payload);
    }

    #[test]
    fn rejects_short_buffer() {
        assert!(matches!(
            BinaryFrame::decode(&[0x01, 0x00]),
            Err(FrameError::TooShort { .. })
        ));
    }

    #[test]
    fn rejects_unknown_kind() {
        let mut bytes = vec![0u8; HEADER_LEN];
        bytes[0] = 0x99;
        assert!(matches!(
            BinaryFrame::decode(&bytes),
            Err(FrameError::UnknownKind(0x99))
        ));
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p omnireach-protocol frame`
Expected: FAIL — `cannot find type 'BinaryFrame' in this scope`

- [ ] **Step 3: Implement the codec**

Prepend to `crates/protocol/src/frame.rs`:

```rust
use thiserror::Error;

pub const HEADER_LEN: usize = 13;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[repr(u8)]
pub enum FrameKind {
    PtyOutput = 0x01,
    PtyInput = 0x02,
    FsChunk = 0x03,
}

impl FrameKind {
    fn from_u8(v: u8) -> Option<Self> {
        match v {
            0x01 => Some(Self::PtyOutput),
            0x02 => Some(Self::PtyInput),
            0x03 => Some(Self::FsChunk),
            _ => None,
        }
    }
}

#[derive(Debug, Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("frame too short: got {got} bytes, need at least {HEADER_LEN}")]
    TooShort { got: usize },
    #[error("unknown frame kind: {0:#04x}")]
    UnknownKind(u8),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BinaryFrame {
    pub kind: FrameKind,
    pub handle: u32,
    pub seq: u64,
    pub payload: Vec<u8>,
}

impl BinaryFrame {
    pub fn encode(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(HEADER_LEN + self.payload.len());
        out.push(self.kind as u8);
        out.extend_from_slice(&self.handle.to_le_bytes());
        out.extend_from_slice(&self.seq.to_le_bytes());
        out.extend_from_slice(&self.payload);
        out
    }

    pub fn decode(bytes: &[u8]) -> Result<Self, FrameError> {
        if bytes.len() < HEADER_LEN {
            return Err(FrameError::TooShort { got: bytes.len() });
        }
        let kind = FrameKind::from_u8(bytes[0]).ok_or(FrameError::UnknownKind(bytes[0]))?;
        let handle = u32::from_le_bytes(bytes[1..5].try_into().expect("4 bytes"));
        let seq = u64::from_le_bytes(bytes[5..13].try_into().expect("8 bytes"));
        Ok(Self {
            kind,
            handle,
            seq,
            payload: bytes[HEADER_LEN..].to_vec(),
        })
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-protocol frame`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/src/frame.rs
git commit -m "feat: binary frame codec for multiplexed bulk data"
```

---

### Task 4: Control message types

**Files:**
- Create/replace: `crates/protocol/src/control.rs`

**Interfaces:**
- Produces (used by Tasks 6, 9, 12):
  - `ControlRequest { id: u64, op: String, params: serde_json::Value }`
  - `ControlResponse::ok(id, result) -> ControlResponse` / `ControlResponse::err(id, code, message)`
  - `ControlEvent` enum, tagged by `event`: `PtyExit { session_id: Uuid, status: i32 }`, `PtyResync { session_id: Uuid, from_seq: u64 }`
  - `PtyCreateParams { shell: Option<String>, cwd: Option<String>, cols: u16, rows: u16 }`
  - `PtyAttachParams { session_id: Uuid, since: u64 }`
  - `PtyResizeParams { handle: u32, cols: u16, rows: u16 }`
  - `SessionInfo { session_id, shell, cwd, cols, rows, alive, head_seq }`

- [ ] **Step 1: Write the failing tests**

Create `crates/protocol/src/control.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn deserializes_a_request() {
        let json = r#"{"id":42,"op":"pty.create","params":{"cols":120,"rows":40}}"#;
        let req: ControlRequest = serde_json::from_str(json).unwrap();
        assert_eq!(req.id, 42);
        assert_eq!(req.op, "pty.create");
        let p: PtyCreateParams = serde_json::from_value(req.params).unwrap();
        assert_eq!(p.cols, 120);
        assert_eq!(p.rows, 40);
        assert!(p.shell.is_none());
    }

    #[test]
    fn serializes_ok_response() {
        let r = ControlResponse::ok(42, serde_json::json!({"handle": 1}));
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["id"], 42);
        assert_eq!(v["ok"], true);
        assert_eq!(v["result"]["handle"], 1);
        assert!(v.get("error").is_none(), "ok response must omit error");
    }

    #[test]
    fn serializes_error_response() {
        let r = ControlResponse::err(7, "SHELL_NOT_FOUND", "pwsh.exe not on PATH");
        let v: serde_json::Value = serde_json::to_value(&r).unwrap();
        assert_eq!(v["ok"], false);
        assert_eq!(v["error"]["code"], "SHELL_NOT_FOUND");
        assert_eq!(v["error"]["message"], "pwsh.exe not on PATH");
        assert!(v.get("result").is_none(), "error response must omit result");
    }

    #[test]
    fn serializes_events_with_event_tag() {
        let id = uuid::Uuid::nil();
        let v = serde_json::to_value(ControlEvent::PtyExit { session_id: id, status: 0 }).unwrap();
        assert_eq!(v["event"], "pty.exit");
        assert_eq!(v["status"], 0);

        let v = serde_json::to_value(ControlEvent::PtyResync { session_id: id, from_seq: 918_233 }).unwrap();
        assert_eq!(v["event"], "pty.resync");
        assert_eq!(v["from_seq"], 918_233);
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p omnireach-protocol control`
Expected: FAIL — unresolved types.

- [ ] **Step 3: Implement the types**

Prepend to `crates/protocol/src/control.rs`:

```rust
use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Deserialize)]
pub struct ControlRequest {
    pub id: u64,
    pub op: String,
    #[serde(default)]
    pub params: serde_json::Value,
}

#[derive(Debug, Clone, Serialize)]
pub struct ProtocolError {
    pub code: String,
    pub message: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ControlResponse {
    pub id: u64,
    pub ok: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub result: Option<serde_json::Value>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<ProtocolError>,
}

impl ControlResponse {
    pub fn ok(id: u64, result: serde_json::Value) -> Self {
        Self { id, ok: true, result: Some(result), error: None }
    }

    pub fn err(id: u64, code: impl Into<String>, message: impl Into<String>) -> Self {
        Self {
            id,
            ok: false,
            result: None,
            error: Some(ProtocolError { code: code.into(), message: message.into() }),
        }
    }
}

#[derive(Debug, Clone, Serialize)]
#[serde(tag = "event")]
pub enum ControlEvent {
    #[serde(rename = "pty.exit")]
    PtyExit { session_id: Uuid, status: i32 },
    #[serde(rename = "pty.resync")]
    PtyResync { session_id: Uuid, from_seq: u64 },
}

#[derive(Debug, Clone, Deserialize)]
pub struct PtyCreateParams {
    #[serde(default)]
    pub shell: Option<String>,
    #[serde(default)]
    pub cwd: Option<String>,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PtyAttachParams {
    pub session_id: Uuid,
    #[serde(default)]
    pub since: u64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct PtyResizeParams {
    pub handle: u32,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Clone, Serialize)]
pub struct SessionInfo {
    pub session_id: Uuid,
    pub shell: String,
    pub cwd: String,
    pub cols: u16,
    pub rows: u16,
    pub alive: bool,
    pub head_seq: u64,
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-protocol`
Expected: PASS, all frame + control tests.

- [ ] **Step 5: Commit**

```bash
git add crates/protocol/src/control.rs
git commit -m "feat: control message types for the wire protocol"
```

---

### Task 5: PTY session

**Files:**
- Create: `crates/host/src/pty.rs`
- Modify: `crates/host/src/main.rs` (add `mod pty;`)

**Interfaces:**
- Consumes: `ring::Ring` from Task 2.
- Produces:
  - `PtySession::spawn(spec: &PtySpec, scrollback: usize) -> Result<Arc<PtySession>, PtyError>`
  - `PtySpec { shell: String, args: Vec<String>, cwd: Option<String>, cols: u16, rows: u16 }`
  - `PtySession::id(&self) -> Uuid`
  - `PtySession::write_input(&self, bytes: &[u8]) -> Result<(), PtyError>`
  - `PtySession::resize(&self, cols: u16, rows: u16) -> Result<(), PtyError>`
  - `PtySession::read_since(&self, seq: u64) -> (u64, Vec<u8>)`
  - `PtySession::head_seq(&self) -> u64`
  - `PtySession::subscribe(&self) -> broadcast::Receiver<Arc<Vec<u8>>>`
  - `PtySession::is_alive(&self) -> bool`
  - `PtySession::kill(&self) -> Result<(), PtyError>`

**Design note.** `portable_pty` is blocking and its reader is a `Box<dyn Read + Send>`. The read loop therefore runs on `tokio::task::spawn_blocking`, appending to the ring under a `parking_lot::Mutex` and publishing each chunk to a `broadcast` channel. Chunks are `Arc<Vec<u8>>` so fanout to N connections does not copy.

**Windows note.** `native_pty_system()` returns a ConPTY-backed implementation on Windows and a Unix PTY here. This task is exercised on macOS only; ConPTY behavior is unverified.

- [ ] **Step 1: Write the failing tests**

Create `crates/host/src/pty.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use std::time::Duration;

    fn echo_spec(text: &str) -> PtySpec {
        // Unix-only test shell; the Windows path is type-checked, not run.
        PtySpec {
            shell: "/bin/sh".into(),
            args: vec!["-c".into(), format!("printf '%s' '{text}'")],
            cwd: None,
            cols: 80,
            rows: 24,
        }
    }

    #[tokio::test]
    async fn captures_child_output_into_the_ring() {
        let s = PtySession::spawn(&echo_spec("hello-omnireach"), 64 * 1024).unwrap();
        // Give the blocking reader a moment to drain the child.
        for _ in 0..50 {
            if s.head_seq() > 0 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        let (start, bytes) = s.read_since(0);
        assert_eq!(start, 0);
        let text = String::from_utf8_lossy(&bytes);
        assert!(text.contains("hello-omnireach"), "got: {text:?}");
    }

    #[tokio::test]
    async fn subscribers_receive_live_output() {
        let spec = PtySpec {
            shell: "/bin/sh".into(),
            args: vec!["-c".into(), "sleep 0.2; printf 'late-output'".into()],
            cwd: None,
            cols: 80,
            rows: 24,
        };
        let s = PtySession::spawn(&spec, 64 * 1024).unwrap();
        let mut rx = s.subscribe();
        let chunk = tokio::time::timeout(Duration::from_secs(5), rx.recv())
            .await
            .expect("timed out waiting for output")
            .expect("broadcast closed");
        assert!(String::from_utf8_lossy(&chunk).contains("late-output"));
    }

    #[tokio::test]
    async fn reports_death_after_the_child_exits() {
        let s = PtySession::spawn(&echo_spec("bye"), 4096).unwrap();
        for _ in 0..100 {
            if !s.is_alive() {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        assert!(!s.is_alive(), "session should report dead after child exits");
    }

    #[tokio::test]
    async fn accepts_input_and_echoes_it_back() {
        let spec = PtySpec {
            shell: "/bin/sh".into(),
            args: vec![],
            cwd: None,
            cols: 80,
            rows: 24,
        };
        let s = PtySession::spawn(&spec, 64 * 1024).unwrap();
        tokio::time::sleep(Duration::from_millis(200)).await;
        s.write_input(b"printf 'from-stdin'\n").unwrap();

        let mut found = false;
        for _ in 0..100 {
            let (_, bytes) = s.read_since(0);
            if String::from_utf8_lossy(&bytes).contains("from-stdin") {
                found = true;
                break;
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        assert!(found, "expected shell to process written input");
    }

    #[tokio::test]
    async fn resize_does_not_error() {
        let spec = PtySpec {
            shell: "/bin/sh".into(),
            args: vec![],
            cwd: None,
            cols: 80,
            rows: 24,
        };
        let s = PtySession::spawn(&spec, 4096).unwrap();
        s.resize(120, 40).unwrap();
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p omnireach-host pty`
Expected: FAIL — `cannot find type 'PtySession'`.

- [ ] **Step 3: Implement `PtySession`**

Prepend to `crates/host/src/pty.rs`:

```rust
use crate::ring::Ring;
use parking_lot::Mutex;
use portable_pty::{CommandBuilder, MasterPty, PtySize, native_pty_system};
use std::io::{Read, Write};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use thiserror::Error;
use tokio::sync::broadcast;
use uuid::Uuid;

const BROADCAST_CAPACITY: usize = 256;
const READ_CHUNK: usize = 8 * 1024;

#[derive(Debug, Clone)]
pub struct PtySpec {
    pub shell: String,
    pub args: Vec<String>,
    pub cwd: Option<String>,
    pub cols: u16,
    pub rows: u16,
}

#[derive(Debug, Error)]
pub enum PtyError {
    #[error("failed to open pty: {0}")]
    Open(String),
    #[error("failed to spawn '{shell}': {source}")]
    Spawn { shell: String, source: String },
    #[error("failed to write to pty: {0}")]
    Write(String),
    #[error("failed to resize pty: {0}")]
    Resize(String),
    #[error("failed to kill child: {0}")]
    Kill(String),
}

pub struct PtySession {
    id: Uuid,
    spec: PtySpec,
    ring: Mutex<Ring>,
    tx: broadcast::Sender<Arc<Vec<u8>>>,
    master: Mutex<Box<dyn MasterPty + Send>>,
    writer: Mutex<Box<dyn Write + Send>>,
    killer: Mutex<Box<dyn portable_pty::ChildKiller + Send + Sync>>,
    alive: AtomicBool,
    size: Mutex<(u16, u16)>,
}

impl PtySession {
    pub fn spawn(spec: &PtySpec, scrollback: usize) -> Result<Arc<Self>, PtyError> {
        let pty_system = native_pty_system();
        let size = PtySize {
            rows: spec.rows,
            cols: spec.cols,
            pixel_width: 0,
            pixel_height: 0,
        };
        let pair = pty_system
            .openpty(size)
            .map_err(|e| PtyError::Open(e.to_string()))?;

        let mut cmd = CommandBuilder::new(&spec.shell);
        for a in &spec.args {
            cmd.arg(a);
        }
        if let Some(cwd) = &spec.cwd {
            cmd.cwd(cwd);
        }

        let mut child = pair.slave.spawn_command(cmd).map_err(|e| PtyError::Spawn {
            shell: spec.shell.clone(),
            source: e.to_string(),
        })?;
        // Dropping the slave here is important: otherwise the master read
        // never sees EOF when the child exits.
        drop(pair.slave);

        let killer = child.clone_killer();
        let reader = pair
            .master
            .try_clone_reader()
            .map_err(|e| PtyError::Open(e.to_string()))?;
        let writer = pair
            .master
            .take_writer()
            .map_err(|e| PtyError::Open(e.to_string()))?;

        let (tx, _rx) = broadcast::channel(BROADCAST_CAPACITY);

        let session = Arc::new(Self {
            id: Uuid::new_v4(),
            spec: spec.clone(),
            ring: Mutex::new(Ring::new(scrollback)),
            tx,
            master: Mutex::new(pair.master),
            writer: Mutex::new(writer),
            killer: Mutex::new(killer),
            alive: AtomicBool::new(true),
            size: Mutex::new((spec.cols, spec.rows)),
        });

        // Blocking read loop. portable-pty's reader has no async interface.
        {
            let session = Arc::clone(&session);
            let mut reader = reader;
            tokio::task::spawn_blocking(move || {
                let mut buf = vec![0u8; READ_CHUNK];
                loop {
                    match reader.read(&mut buf) {
                        Ok(0) => break,
                        Ok(n) => {
                            // Bytes only. Never decode here — a multi-byte
                            // sequence split across reads would be corrupted
                            // irrecoverably in the ring.
                            let chunk = Arc::new(buf[..n].to_vec());
                            session.ring.lock().write(&chunk);
                            let _ = session.tx.send(chunk);
                        }
                        Err(_) => break,
                    }
                }
                session.alive.store(false, Ordering::SeqCst);
            });
        }

        // Reap the child so `is_alive` flips even if the reader lingers.
        {
            let session = Arc::clone(&session);
            tokio::task::spawn_blocking(move || {
                let _ = child.wait();
                session.alive.store(false, Ordering::SeqCst);
            });
        }

        Ok(session)
    }

    pub fn id(&self) -> Uuid {
        self.id
    }

    pub fn spec(&self) -> &PtySpec {
        &self.spec
    }

    pub fn size(&self) -> (u16, u16) {
        *self.size.lock()
    }

    pub fn is_alive(&self) -> bool {
        self.alive.load(Ordering::SeqCst)
    }

    pub fn head_seq(&self) -> u64 {
        self.ring.lock().head_seq()
    }

    pub fn read_since(&self, seq: u64) -> (u64, Vec<u8>) {
        self.ring.lock().read_since(seq)
    }

    pub fn subscribe(&self) -> broadcast::Receiver<Arc<Vec<u8>>> {
        self.tx.subscribe()
    }

    pub fn write_input(&self, bytes: &[u8]) -> Result<(), PtyError> {
        let mut w = self.writer.lock();
        w.write_all(bytes).map_err(|e| PtyError::Write(e.to_string()))?;
        w.flush().map_err(|e| PtyError::Write(e.to_string()))
    }

    pub fn resize(&self, cols: u16, rows: u16) -> Result<(), PtyError> {
        self.master
            .lock()
            .resize(PtySize { rows, cols, pixel_width: 0, pixel_height: 0 })
            .map_err(|e| PtyError::Resize(e.to_string()))?;
        *self.size.lock() = (cols, rows);
        Ok(())
    }

    pub fn kill(&self) -> Result<(), PtyError> {
        self.killer
            .lock()
            .kill()
            .map_err(|e| PtyError::Kill(e.to_string()))
    }
}
```

Add `mod pty;` to `crates/host/src/main.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-host pty -- --test-threads=4`
Expected: PASS, 5 tests.

If `clone_killer` or `ChildKiller` bounds do not match, run `cargo doc -p portable-pty --open` and adjust — the trait object bounds are the most likely point of friction. Do not work around it by decoding bytes or by dropping the kill capability.

- [ ] **Step 5: Verify Windows target still checks**

Run: `cargo check --all --target x86_64-pc-windows-msvc`
Expected: success. This is the only signal we get that the ConPTY code path compiles.

- [ ] **Step 6: Commit**

```bash
git add crates/host/src/pty.rs crates/host/src/main.rs
git commit -m "feat: PTY session with ring-backed scrollback and broadcast fanout"
```

---

### Task 6: Session registry

**Files:**
- Create: `crates/host/src/sessions.rs`
- Modify: `crates/host/src/main.rs` (add `mod sessions;`)

**Interfaces:**
- Consumes: `PtySession`, `PtySpec`, `PtyError` from Task 5.
- Produces:
  - `SessionRegistry::new(scrollback: usize) -> SessionRegistry`
  - `SessionRegistry::create(&self, spec: PtySpec) -> Result<Arc<PtySession>, PtyError>`
  - `SessionRegistry::get(&self, id: Uuid) -> Option<Arc<PtySession>>`
  - `SessionRegistry::list(&self) -> Vec<SessionInfo>`
  - `SessionRegistry::kill(&self, id: Uuid) -> Result<(), PtyError>`
  - `SessionRegistry::reap_dead(&self)` — removes dead sessions whose output has been fully consumed

- [ ] **Step 1: Write the failing tests**

Create `crates/host/src/sessions.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;
    use crate::pty::PtySpec;

    fn sh(script: &str) -> PtySpec {
        PtySpec {
            shell: "/bin/sh".into(),
            args: vec!["-c".into(), script.into()],
            cwd: None,
            cols: 80,
            rows: 24,
        }
    }

    #[tokio::test]
    async fn create_then_get_returns_the_same_session() {
        let reg = SessionRegistry::new(4096);
        let s = reg.create(sh("sleep 1")).unwrap();
        let got = reg.get(s.id()).expect("session should be retrievable");
        assert_eq!(got.id(), s.id());
    }

    #[tokio::test]
    async fn list_reports_every_live_session() {
        let reg = SessionRegistry::new(4096);
        let a = reg.create(sh("sleep 1")).unwrap();
        let b = reg.create(sh("sleep 1")).unwrap();
        let ids: Vec<_> = reg.list().into_iter().map(|i| i.session_id).collect();
        assert!(ids.contains(&a.id()));
        assert!(ids.contains(&b.id()));
        assert_eq!(ids.len(), 2);
    }

    #[tokio::test]
    async fn get_returns_none_for_unknown_id() {
        let reg = SessionRegistry::new(4096);
        assert!(reg.get(uuid::Uuid::new_v4()).is_none());
    }

    #[tokio::test]
    async fn killed_session_stops_reporting_alive() {
        let reg = SessionRegistry::new(4096);
        let s = reg.create(sh("sleep 30")).unwrap();
        reg.kill(s.id()).unwrap();
        for _ in 0..100 {
            if !s.is_alive() {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert!(!s.is_alive());
    }

    #[tokio::test]
    async fn session_survives_dropping_every_subscriber() {
        // This is the registry-level statement of the whole product thesis:
        // sessions belong to the host, not to any connection.
        let reg = SessionRegistry::new(64 * 1024);
        let s = reg.create(sh("sleep 0.1; printf 'still-here'")).unwrap();
        drop(s.subscribe()); // simulate a client attaching then vanishing

        for _ in 0..100 {
            let (_, b) = s.read_since(0);
            if String::from_utf8_lossy(&b).contains("still-here") {
                return;
            }
            tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        }
        panic!("session output was lost when the subscriber went away");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p omnireach-host sessions`
Expected: FAIL — `cannot find type 'SessionRegistry'`.

- [ ] **Step 3: Implement the registry**

Prepend to `crates/host/src/sessions.rs`:

```rust
use crate::pty::{PtyError, PtySession, PtySpec};
use omnireach_protocol::control::SessionInfo;
use parking_lot::RwLock;
use std::collections::HashMap;
use std::sync::Arc;
use uuid::Uuid;

pub struct SessionRegistry {
    sessions: RwLock<HashMap<Uuid, Arc<PtySession>>>,
    scrollback: usize,
}

impl SessionRegistry {
    pub fn new(scrollback: usize) -> Self {
        Self { sessions: RwLock::new(HashMap::new()), scrollback }
    }

    pub fn create(&self, spec: PtySpec) -> Result<Arc<PtySession>, PtyError> {
        let session = PtySession::spawn(&spec, self.scrollback)?;
        self.sessions.write().insert(session.id(), Arc::clone(&session));
        Ok(session)
    }

    pub fn get(&self, id: Uuid) -> Option<Arc<PtySession>> {
        self.sessions.read().get(&id).cloned()
    }

    pub fn list(&self) -> Vec<SessionInfo> {
        self.sessions
            .read()
            .values()
            .map(|s| {
                let (cols, rows) = s.size();
                SessionInfo {
                    session_id: s.id(),
                    shell: s.spec().shell.clone(),
                    cwd: s.spec().cwd.clone().unwrap_or_default(),
                    cols,
                    rows,
                    alive: s.is_alive(),
                    head_seq: s.head_seq(),
                }
            })
            .collect()
    }

    pub fn kill(&self, id: Uuid) -> Result<(), PtyError> {
        match self.get(id) {
            Some(s) => s.kill(),
            None => Ok(()),
        }
    }

    /// Drop dead sessions. Called on a timer by the host; dead sessions are
    /// retained (not removed on exit) so a client that reconnects late can
    /// still read the final output of a finished task.
    pub fn reap_dead(&self) {
        self.sessions.write().retain(|_, s| s.is_alive());
    }
}
```

Add `mod sessions;` to `crates/host/src/main.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-host sessions`
Expected: PASS, 5 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/host/src/sessions.rs crates/host/src/main.rs
git commit -m "feat: session registry owning PTYs independently of connections"
```

---

### Task 7: Config

**Files:**
- Create: `crates/host/src/config.rs`
- Modify: `crates/host/src/main.rs` (add `mod config;`)

**Interfaces:**
- Produces:
  - `Config { listen_addr: String, scrollback_bytes: usize, access: Option<AccessConfig>, shells: Vec<ShellConfig> }`
  - `AccessConfig { team_domain: String, aud: String }`
  - `ShellConfig { name: String, command: String, args: Vec<String>, default: bool }`
  - `Config::load() -> Result<Config, ConfigError>` — reads `%APPDATA%\OmniReach\config.toml` (Windows) or `~/.config/omnireach/config.toml` (Unix), falling back to defaults if absent
  - `Config::from_str(s: &str) -> Result<Config, ConfigError>`
  - `Config::default_shell(&self) -> &ShellConfig`
  - `Config::shell_by_name(&self, name: &str) -> Option<&ShellConfig>`

- [ ] **Step 1: Write the failing tests**

Create `crates/host/src/config.rs` with only the test module:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_a_full_config() {
        let toml = r#"
listen_addr = "127.0.0.1:7717"
scrollback_bytes = 2097152

[access]
team_domain = "myteam.cloudflareaccess.com"
aud = "abc123"

[[shells]]
name = "pwsh"
command = "pwsh.exe"
args = ["-NoLogo"]
default = true

[[shells]]
name = "wsl"
command = "wsl.exe"
args = []
"#;
        let c = Config::from_str(toml).unwrap();
        assert_eq!(c.listen_addr, "127.0.0.1:7717");
        assert_eq!(c.scrollback_bytes, 2_097_152);
        let a = c.access.as_ref().unwrap();
        assert_eq!(a.team_domain, "myteam.cloudflareaccess.com");
        assert_eq!(a.aud, "abc123");
        assert_eq!(c.shells.len(), 2);
        assert_eq!(c.default_shell().name, "pwsh");
        assert_eq!(c.shell_by_name("wsl").unwrap().command, "wsl.exe");
        assert!(c.shell_by_name("nope").is_none());
    }

    #[test]
    fn applies_defaults_for_an_empty_config() {
        let c = Config::from_str("").unwrap();
        assert_eq!(c.listen_addr, "127.0.0.1:7717");
        assert_eq!(c.scrollback_bytes, 2_097_152);
        assert!(c.access.is_none(), "access is optional so M1/M2 can run locally");
        assert!(!c.shells.is_empty(), "must synthesize a platform default shell");
    }

    #[test]
    fn first_shell_is_default_when_none_is_flagged() {
        let toml = r#"
[[shells]]
name = "a"
command = "a.exe"

[[shells]]
name = "b"
command = "b.exe"
"#;
        let c = Config::from_str(toml).unwrap();
        assert_eq!(c.default_shell().name, "a");
    }
}
```

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p omnireach-host config`
Expected: FAIL — `cannot find type 'Config'`.

- [ ] **Step 3: Implement config**

Prepend to `crates/host/src/config.rs`:

```rust
use serde::Deserialize;
use std::path::PathBuf;
use thiserror::Error;

fn default_listen_addr() -> String {
    "127.0.0.1:7717".to_string()
}

fn default_scrollback() -> usize {
    2 * 1024 * 1024
}

#[derive(Debug, Error)]
pub enum ConfigError {
    #[error("could not parse config: {0}")]
    Parse(#[from] toml::de::Error),
    #[error("could not read config file: {0}")]
    Io(#[from] std::io::Error),
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessConfig {
    pub team_domain: String,
    pub aud: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ShellConfig {
    pub name: String,
    pub command: String,
    #[serde(default)]
    pub args: Vec<String>,
    #[serde(default)]
    pub default: bool,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Config {
    #[serde(default = "default_listen_addr")]
    pub listen_addr: String,
    #[serde(default = "default_scrollback")]
    pub scrollback_bytes: usize,
    #[serde(default)]
    pub access: Option<AccessConfig>,
    #[serde(default)]
    pub shells: Vec<ShellConfig>,
}

impl Config {
    pub fn config_path() -> Option<PathBuf> {
        dirs::config_dir().map(|d| d.join("OmniReach").join("config.toml"))
    }

    pub fn load() -> Result<Config, ConfigError> {
        match Self::config_path() {
            Some(p) if p.exists() => Self::from_str(&std::fs::read_to_string(p)?),
            _ => Self::from_str(""),
        }
    }

    #[allow(clippy::should_implement_trait)]
    pub fn from_str(s: &str) -> Result<Config, ConfigError> {
        let mut c: Config = toml::from_str(s)?;
        if c.shells.is_empty() {
            c.shells.push(Self::platform_default_shell());
        }
        Ok(c)
    }

    fn platform_default_shell() -> ShellConfig {
        #[cfg(windows)]
        {
            ShellConfig {
                name: "pwsh".into(),
                command: "powershell.exe".into(),
                args: vec!["-NoLogo".into()],
                default: true,
            }
        }
        #[cfg(not(windows))]
        {
            ShellConfig {
                name: "sh".into(),
                command: std::env::var("SHELL").unwrap_or_else(|_| "/bin/sh".into()),
                args: vec![],
                default: true,
            }
        }
    }

    pub fn default_shell(&self) -> &ShellConfig {
        self.shells
            .iter()
            .find(|s| s.default)
            .or_else(|| self.shells.first())
            .expect("from_str guarantees at least one shell")
    }

    pub fn shell_by_name(&self, name: &str) -> Option<&ShellConfig> {
        self.shells.iter().find(|s| s.name == name)
    }
}
```

Add `mod config;` to `crates/host/src/main.rs`.

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-host config`
Expected: PASS, 3 tests.

- [ ] **Step 5: Commit**

```bash
git add crates/host/src/config.rs crates/host/src/main.rs
git commit -m "feat: TOML config with platform default shells"
```

---

### Task 8: Cloudflare Access JWT validation

**Files:**
- Create: `crates/host/src/auth.rs`
- Modify: `crates/host/src/main.rs` (add `mod auth;`)

**Interfaces:**
- Consumes: `AccessConfig` from Task 7.
- Produces:
  - `Authenticator::new(cfg: AccessConfig) -> Authenticator`
  - `Authenticator::validate(&self, token: &str) -> Result<AccessClaims, AuthError>`
  - `AccessClaims { sub: String, email: Option<String>, exp: u64 }`
  - `AuthError` variants: `Missing`, `Malformed`, `UnknownKid`, `Invalid(String)`, `JwksFetch(String)`

**Critical.** `jsonwebtoken = { version = "10.4", features = ["aws_lc_rs"] }` — see Global Constraints. Without the feature this code compiles and panics at runtime.

- [ ] **Step 1: Write the failing tests**

Create `crates/host/src/auth.rs` with only the test module. These tests use a locally generated RSA key so they need no network:

```rust
#[cfg(test)]
mod tests {
    use super::*;

    fn cfg() -> AccessConfig {
        AccessConfig {
            team_domain: "test.cloudflareaccess.com".into(),
            aud: "test-aud".into(),
        }
    }

    #[test]
    fn rejects_an_empty_token() {
        let a = Authenticator::new(cfg());
        assert!(matches!(a.validate(""), Err(AuthError::Missing)));
    }

    #[test]
    fn rejects_a_malformed_token() {
        let a = Authenticator::new(cfg());
        assert!(matches!(
            a.validate("not-a-jwt"),
            Err(AuthError::Malformed)
        ));
    }

    #[test]
    fn rejects_a_token_with_an_unknown_kid() {
        // Three base64url segments with a header naming a kid we have no key for.
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
        let header = B64.encode(br#"{"alg":"RS256","kid":"nope"}"#);
        let payload = B64.encode(br#"{"sub":"x","aud":"test-aud","exp":9999999999}"#);
        let token = format!("{header}.{payload}.c2ln");

        let a = Authenticator::new(cfg());
        a.seed_keys_for_test(std::collections::HashMap::new());
        assert!(matches!(a.validate(&token), Err(AuthError::UnknownKid)));
    }

    #[test]
    fn validation_is_configured_to_check_aud_iss_and_exp() {
        let a = Authenticator::new(cfg());
        let v = a.validation();
        assert!(v.validate_exp, "must reject expired tokens");
        assert!(v.validate_aud, "must reject tokens for another application");
        assert_eq!(
            v.iss.as_ref().expect("issuer must be pinned").iter().next().unwrap(),
            "https://test.cloudflareaccess.com"
        );
        assert!(v.aud.as_ref().unwrap().contains("test-aud"));
        assert_eq!(v.algorithms, vec![jsonwebtoken::Algorithm::RS256]);
    }
}
```

Add to `crates/host/Cargo.toml` `[dev-dependencies]`: `base64 = "0.22"`.

- [ ] **Step 2: Run the tests to verify they fail**

Run: `cargo test -p omnireach-host auth`
Expected: FAIL — `cannot find type 'Authenticator'`.

- [ ] **Step 3: Implement the authenticator**

Prepend to `crates/host/src/auth.rs`:

```rust
use crate::config::AccessConfig;
use jsonwebtoken::jwk::JwkSet;
use jsonwebtoken::{Algorithm, DecodingKey, Validation, decode, decode_header};
use parking_lot::RwLock;
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::time::{Duration, Instant};
use thiserror::Error;

const MIN_REFRESH_INTERVAL: Duration = Duration::from_secs(300);

#[derive(Debug, Error)]
pub enum AuthError {
    #[error("no Access token presented")]
    Missing,
    #[error("Access token is malformed")]
    Malformed,
    #[error("Access token signed by an unknown key")]
    UnknownKid,
    #[error("Access token rejected: {0}")]
    Invalid(String),
    #[error("could not fetch JWKS: {0}")]
    JwksFetch(String),
}

#[derive(Debug, Clone, Deserialize)]
pub struct AccessClaims {
    pub sub: String,
    #[serde(default)]
    pub email: Option<String>,
    pub exp: u64,
}

pub struct Authenticator {
    cfg: AccessConfig,
    keys: RwLock<HashMap<String, DecodingKey>>,
    last_refresh: RwLock<Option<Instant>>,
    validation: Validation,
}

impl Authenticator {
    pub fn new(cfg: AccessConfig) -> Self {
        let mut validation = Validation::new(Algorithm::RS256);
        validation.validate_exp = true;
        validation.validate_nbf = true;
        validation.validate_aud = true;
        validation.aud = Some(HashSet::from([cfg.aud.clone()]));
        validation.iss = Some(HashSet::from([format!("https://{}", cfg.team_domain)]));
        validation.algorithms = vec![Algorithm::RS256];

        Self {
            cfg,
            keys: RwLock::new(HashMap::new()),
            last_refresh: RwLock::new(None),
            validation,
        }
    }

    pub fn validation(&self) -> &Validation {
        &self.validation
    }

    #[cfg(test)]
    pub fn seed_keys_for_test(&self, keys: HashMap<String, DecodingKey>) {
        *self.keys.write() = keys;
        *self.last_refresh.write() = Some(Instant::now());
    }

    pub fn jwks_url(&self) -> String {
        format!("https://{}/cdn-cgi/access/certs", self.cfg.team_domain)
    }

    pub async fn refresh_keys(&self) -> Result<(), AuthError> {
        let body: JwkSet = reqwest::get(self.jwks_url())
            .await
            .map_err(|e| AuthError::JwksFetch(e.to_string()))?
            .json()
            .await
            .map_err(|e| AuthError::JwksFetch(e.to_string()))?;

        let mut map = HashMap::new();
        for jwk in &body.keys {
            if let Some(kid) = jwk.common.key_id.clone() {
                if let Ok(k) = DecodingKey::from_jwk(jwk) {
                    map.insert(kid, k);
                }
            }
        }
        *self.keys.write() = map;
        *self.last_refresh.write() = Some(Instant::now());
        Ok(())
    }

    fn should_refresh(&self) -> bool {
        match *self.last_refresh.read() {
            None => true,
            Some(t) => t.elapsed() >= MIN_REFRESH_INTERVAL,
        }
    }

    /// Validate synchronously against cached keys. Callers must have
    /// populated the cache via `ensure_keys` first.
    pub fn validate(&self, token: &str) -> Result<AccessClaims, AuthError> {
        if token.is_empty() {
            return Err(AuthError::Missing);
        }
        let header = decode_header(token).map_err(|_| AuthError::Malformed)?;
        let kid = header.kid.ok_or(AuthError::Malformed)?;

        let keys = self.keys.read();
        let key = keys.get(&kid).ok_or(AuthError::UnknownKid)?;

        decode::<AccessClaims>(token, key, &self.validation)
            .map(|d| d.claims)
            .map_err(|e| AuthError::Invalid(e.to_string()))
    }

    /// Refresh keys if stale, then validate. Retries once on `UnknownKid`
    /// because Cloudflare rotates signing keys without warning.
    pub async fn ensure_and_validate(&self, token: &str) -> Result<AccessClaims, AuthError> {
        if self.should_refresh() {
            self.refresh_keys().await?;
        }
        match self.validate(token) {
            Err(AuthError::UnknownKid) => {
                self.refresh_keys().await?;
                self.validate(token)
            }
            other => other,
        }
    }
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-host auth`
Expected: PASS, 4 tests.

- [ ] **Step 5: Prove the crypto provider is actually wired**

This guards the exact trap in Global Constraints. Add to the test module:

```rust
    #[test]
    fn crypto_provider_is_installed_so_decode_does_not_panic() {
        // With no crypto backend feature, jsonwebtoken panics here rather
        // than returning Err. A clean Err proves the backend is present.
        use base64::{Engine, engine::general_purpose::URL_SAFE_NO_PAD as B64};
        let header = B64.encode(br#"{"alg":"RS256","kid":"k1"}"#);
        let payload = B64.encode(br#"{"sub":"x","aud":"test-aud","exp":9999999999}"#);
        let token = format!("{header}.{payload}.aW52YWxpZA");

        let a = Authenticator::new(cfg());
        let key = DecodingKey::from_rsa_components(
            "0vx7agoebGcQSuuPiLJXZptN9nndrQmbXEps2aiAFbWhM78LhWx4cbbfAAtVT86zwu1RK7aPFFxuhDR1L6tSoc_BJECPebWKRXjBZCiFV4n3oknjhMstn64tZ_2W-5JsGY4Hc5n9yBXArwl93lqt7_RN5w6Cf0h4QyQ5v-65YGjQR0_FDW2QvzqY368QQMicAtaSqzs8KJZgnYb9c7d0zgdAZHzu6qMQvRL5hajrn1n91CbOpbISD08qNLyrdkt-bFTWhAI4vMQFh6WeZu0fM4lFd2NcRwr3XPksINHaQ-G_xBniIqbw0Ls1jF44-csFCur-kEgU8awapJzKnqDKgw",
            "AQAB",
        )
        .unwrap();
        a.seed_keys_for_test(HashMap::from([("k1".to_string(), key)]));

        // Must return Err (bad signature), not panic.
        let result = a.validate(&token);
        assert!(matches!(result, Err(AuthError::Invalid(_))), "got {result:?}");
    }
```

Run: `cargo test -p omnireach-host auth::tests::crypto_provider_is_installed`
Expected: PASS. If this test **panics** rather than failing, the `aws_lc_rs` feature is missing from `Cargo.toml`.

- [ ] **Step 6: Commit**

```bash
git add crates/host/src/auth.rs crates/host/src/main.rs crates/host/Cargo.toml
git commit -m "feat: Cloudflare Access JWT validation with JWKS caching"
```

---

### Task 9: WebSocket transport with backpressure

The busiest file in the host. Handles the WS upgrade, dispatches control ops, fans PTY output out to the socket, and implements the resync path.

**Files:**
- Create: `crates/host/src/transport.rs`
- Modify: `crates/host/src/main.rs` (add `mod transport;`)

**Interfaces:**
- Consumes: `SessionRegistry` (Task 6), `Config` (Task 7), `Authenticator` (Task 8), `BinaryFrame`/`ControlRequest`/`ControlResponse`/`ControlEvent` (Tasks 3–4).
- Produces:
  - `AppState { registry: Arc<SessionRegistry>, config: Arc<Config>, auth: Option<Arc<Authenticator>> }`
  - `router(state: AppState) -> axum::Router`
  - Routes: `GET /health`, `GET /ws`

**Backpressure rule.** Per-connection outbound queue capped at `SEND_QUEUE_HIGH_WATER` (4 MiB accounted by payload bytes). On overflow for a session: drop that session's queued output frames, emit `ControlEvent::PtyResync { from_seq }`, and send the ring tail as one frame.

- [ ] **Step 1: Write the failing integration test**

Create `crates/host/tests/transport.rs`:

```rust
use futures_util::{SinkExt, StreamExt};
use omnireach_protocol::{BinaryFrame, FrameKind};
use tokio_tungstenite::tungstenite::Message;

async fn start_test_host() -> String {
    let config = std::sync::Arc::new(omnireach_host::config::Config::from_str("").unwrap());
    let registry = std::sync::Arc::new(omnireach_host::sessions::SessionRegistry::new(64 * 1024));
    let state = omnireach_host::transport::AppState { registry, config, auth: None };

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, omnireach_host::transport::router(state))
            .await
            .unwrap();
    });
    format!("ws://{addr}/ws")
}

#[tokio::test]
async fn creates_a_pty_and_streams_its_output() {
    let url = start_test_host().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();

    ws.send(Message::Text(
        serde_json::json!({
            "id": 1,
            "op": "pty.create",
            "params": {"shell":"sh","cwd":null,"cols":80,"rows":24}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    // First response must be the ok for id 1.
    let handle = loop {
        match ws.next().await.unwrap().unwrap() {
            Message::Text(t) => {
                let v: serde_json::Value = serde_json::from_str(&t).unwrap();
                if v["id"] == 1 {
                    assert_eq!(v["ok"], true, "create failed: {v}");
                    break v["result"]["handle"].as_u64().unwrap() as u32;
                }
            }
            _ => continue,
        }
    };

    // Send input, expect it echoed back as binary PtyOutput frames.
    let input = BinaryFrame {
        kind: FrameKind::PtyInput,
        handle,
        seq: 0,
        payload: b"printf 'ws-roundtrip'\n".to_vec(),
    };
    ws.send(Message::Binary(input.encode().into())).await.unwrap();

    let mut seen = Vec::new();
    let deadline = tokio::time::Instant::now() + std::time::Duration::from_secs(10);
    while tokio::time::Instant::now() < deadline {
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), ws.next()).await;
        let Ok(Some(Ok(Message::Binary(b)))) = msg else { continue };
        let f = BinaryFrame::decode(&b).unwrap();
        if f.kind == FrameKind::PtyOutput {
            seen.extend_from_slice(&f.payload);
            if String::from_utf8_lossy(&seen).contains("ws-roundtrip") {
                return;
            }
        }
    }
    panic!("never saw echoed output; got {:?}", String::from_utf8_lossy(&seen));
}

#[tokio::test]
async fn health_endpoint_responds() {
    let url = start_test_host().await;
    let http = url.replace("ws://", "http://").replace("/ws", "/health");
    let body = reqwest::get(&http).await.unwrap().text().await.unwrap();
    assert!(body.contains("ok"), "got {body}");
}
```

This requires the host to be a **library plus binary**. Add to `crates/host/Cargo.toml`:

```toml
[lib]
name = "omnireach_host"
path = "src/lib.rs"

[[bin]]
name = "omnireach-host"
path = "src/main.rs"
```

Add `reqwest` to `[dev-dependencies]` as well.

Create `crates/host/src/lib.rs`:

```rust
pub mod auth;
pub mod config;
pub mod pty;
pub mod ring;
pub mod sessions;
pub mod transport;
```

And reduce `crates/host/src/main.rs` to use the library (see Task 10).

- [ ] **Step 2: Run the test to verify it fails**

Run: `cargo test -p omnireach-host --test transport`
Expected: FAIL — `transport` module does not exist.

- [ ] **Step 3: Implement the transport**

Create `crates/host/src/transport.rs`:

```rust
use crate::auth::Authenticator;
use crate::config::Config;
use crate::pty::{PtySession, PtySpec};
use crate::sessions::SessionRegistry;
use axum::extract::ws::{Message, WebSocket, WebSocketUpgrade};
use axum::extract::State;
use axum::http::{HeaderMap, StatusCode};
use axum::response::IntoResponse;
use axum::routing::get;
use axum::Router;
use futures_util::{SinkExt, StreamExt};
use omnireach_protocol::control::{
    ControlEvent, ControlRequest, ControlResponse, PtyAttachParams, PtyCreateParams,
    PtyResizeParams,
};
use omnireach_protocol::{BinaryFrame, FrameKind};
use std::collections::HashMap;
use std::sync::Arc;
use tokio::sync::mpsc;
use uuid::Uuid;

const SEND_QUEUE_HIGH_WATER: usize = 4 * 1024 * 1024;
const OUTBOUND_CHANNEL_DEPTH: usize = 1024;

#[derive(Clone)]
pub struct AppState {
    pub registry: Arc<SessionRegistry>,
    pub config: Arc<Config>,
    pub auth: Option<Arc<Authenticator>>,
}

pub fn router(state: AppState) -> Router {
    Router::new()
        .route("/health", get(health))
        .route("/ws", get(ws_upgrade))
        .with_state(state)
}

async fn health() -> impl IntoResponse {
    (StatusCode::OK, r#"{"status":"ok"}"#)
}

async fn ws_upgrade(
    State(state): State<AppState>,
    headers: HeaderMap,
    ws: WebSocketUpgrade,
) -> impl IntoResponse {
    if let Some(auth) = &state.auth {
        let token = headers
            .get("cf-access-jwt-assertion")
            .and_then(|v| v.to_str().ok())
            .unwrap_or("");
        if let Err(e) = auth.ensure_and_validate(token).await {
            tracing::warn!("rejected websocket upgrade: {e}");
            return (StatusCode::UNAUTHORIZED, "unauthorized").into_response();
        }
    }
    ws.on_upgrade(move |socket| handle_socket(socket, state))
}

/// What the writer task is asked to send.
enum Outbound {
    Text(String),
    Binary(Vec<u8>),
}

struct Attachment {
    session: Arc<PtySession>,
    handle: u32,
}

async fn handle_socket(socket: WebSocket, state: AppState) {
    let (mut sink, mut stream) = socket.split();
    let (tx, mut rx) = mpsc::channel::<Outbound>(OUTBOUND_CHANNEL_DEPTH);

    // Writer task: the only thing that touches the sink.
    let writer = tokio::spawn(async move {
        while let Some(msg) = rx.recv().await {
            let m = match msg {
                Outbound::Text(t) => Message::Text(t.into()),
                Outbound::Binary(b) => Message::Binary(b.into()),
            };
            if sink.send(m).await.is_err() {
                break;
            }
        }
    });

    let mut attachments: HashMap<u32, Attachment> = HashMap::new();
    let mut next_handle: u32 = 1;
    let mut pumps: Vec<tokio::task::JoinHandle<()>> = Vec::new();

    while let Some(Ok(msg)) = stream.next().await {
        match msg {
            Message::Text(text) => {
                let Ok(req) = serde_json::from_str::<ControlRequest>(&text) else {
                    continue;
                };
                let resp = handle_control(
                    &state,
                    &req,
                    &mut attachments,
                    &mut next_handle,
                    &tx,
                    &mut pumps,
                );
                let _ = tx.send(Outbound::Text(serde_json::to_string(&resp).unwrap())).await;
            }
            Message::Binary(bytes) => {
                let Ok(frame) = BinaryFrame::decode(&bytes) else { continue };
                if frame.kind != FrameKind::PtyInput {
                    continue;
                }
                if let Some(a) = attachments.get(&frame.handle) {
                    let _ = a.session.write_input(&frame.payload);
                }
            }
            Message::Close(_) => break,
            _ => {}
        }
    }

    for p in pumps {
        p.abort();
    }
    writer.abort();
}

fn handle_control(
    state: &AppState,
    req: &ControlRequest,
    attachments: &mut HashMap<u32, Attachment>,
    next_handle: &mut u32,
    tx: &mpsc::Sender<Outbound>,
    pumps: &mut Vec<tokio::task::JoinHandle<()>>,
) -> ControlResponse {
    match req.op.as_str() {
        "pty.list" => match serde_json::to_value(state.registry.list()) {
            Ok(v) => ControlResponse::ok(req.id, serde_json::json!({ "sessions": v })),
            Err(e) => ControlResponse::err(req.id, "INTERNAL", e.to_string()),
        },

        "pty.create" => {
            let p: PtyCreateParams = match serde_json::from_value(req.params.clone()) {
                Ok(p) => p,
                Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
            };
            let shell_cfg = match &p.shell {
                Some(name) => match state.config.shell_by_name(name) {
                    Some(s) => s,
                    None => {
                        return ControlResponse::err(
                            req.id,
                            "SHELL_NOT_FOUND",
                            format!("no shell named '{name}' in config"),
                        )
                    }
                },
                None => state.config.default_shell(),
            };
            let spec = PtySpec {
                shell: shell_cfg.command.clone(),
                args: shell_cfg.args.clone(),
                cwd: p.cwd.clone(),
                cols: p.cols,
                rows: p.rows,
            };
            match state.registry.create(spec) {
                Ok(session) => {
                    let handle = *next_handle;
                    *next_handle += 1;
                    let session_id = session.id();
                    pumps.push(spawn_pump(Arc::clone(&session), handle, 0, tx.clone()));
                    attachments.insert(handle, Attachment { session, handle });
                    ControlResponse::ok(
                        req.id,
                        serde_json::json!({ "session_id": session_id, "handle": handle }),
                    )
                }
                Err(e) => ControlResponse::err(req.id, "SPAWN_FAILED", e.to_string()),
            }
        }

        "pty.attach" => {
            let p: PtyAttachParams = match serde_json::from_value(req.params.clone()) {
                Ok(p) => p,
                Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
            };
            let Some(session) = state.registry.get(p.session_id) else {
                return ControlResponse::err(req.id, "NO_SUCH_SESSION", "unknown session_id");
            };
            let handle = *next_handle;
            *next_handle += 1;
            let (start, _) = session.read_since(p.since);
            let truncated_from = if start > p.since { Some(start) } else { None };
            let (cols, rows) = session.size();

            pumps.push(spawn_pump(Arc::clone(&session), handle, p.since, tx.clone()));
            attachments.insert(handle, Attachment { session, handle });

            ControlResponse::ok(
                req.id,
                serde_json::json!({
                    "handle": handle,
                    "cols": cols,
                    "rows": rows,
                    "truncated_from": truncated_from,
                }),
            )
        }

        "pty.detach" => {
            let handle = req.params.get("handle").and_then(|v| v.as_u64()).unwrap_or(0) as u32;
            attachments.remove(&handle);
            ControlResponse::ok(req.id, serde_json::json!({}))
        }

        "pty.resize" => {
            let p: PtyResizeParams = match serde_json::from_value(req.params.clone()) {
                Ok(p) => p,
                Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
            };
            match attachments.get(&p.handle) {
                Some(a) => match a.session.resize(p.cols, p.rows) {
                    Ok(()) => ControlResponse::ok(req.id, serde_json::json!({})),
                    Err(e) => ControlResponse::err(req.id, "RESIZE_FAILED", e.to_string()),
                },
                None => ControlResponse::err(req.id, "NO_SUCH_HANDLE", "unknown handle"),
            }
        }

        "pty.kill" => {
            let id: Uuid = match req.params.get("session_id").and_then(|v| v.as_str()) {
                Some(s) => match s.parse() {
                    Ok(id) => id,
                    Err(e) => return ControlResponse::err(req.id, "BAD_PARAMS", e.to_string()),
                },
                None => return ControlResponse::err(req.id, "BAD_PARAMS", "session_id required"),
            };
            match state.registry.kill(id) {
                Ok(()) => ControlResponse::ok(req.id, serde_json::json!({})),
                Err(e) => ControlResponse::err(req.id, "KILL_FAILED", e.to_string()),
            }
        }

        other => ControlResponse::err(req.id, "UNKNOWN_OP", format!("unknown op '{other}'")),
    }
}

/// Replays `since..head` then streams live output, applying backpressure.
fn spawn_pump(
    session: Arc<PtySession>,
    handle: u32,
    since: u64,
    tx: mpsc::Sender<Outbound>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        // Subscribe BEFORE replaying, so output produced during the replay is
        // queued rather than lost. The overlap is de-duplicated by sequence
        // number below.
        let mut rx = session.subscribe();
        let (start, backlog) = session.read_since(since);
        let mut next_seq = start;

        if !backlog.is_empty() {
            next_seq = start + backlog.len() as u64;
            let frame = BinaryFrame {
                kind: FrameKind::PtyOutput,
                handle,
                seq: start,
                payload: backlog,
            };
            if tx.send(Outbound::Binary(frame.encode())).await.is_err() {
                return;
            }
        }

        let mut queued_bytes: usize = 0;

        loop {
            match rx.recv().await {
                Ok(chunk) => {
                    queued_bytes += chunk.len();

                    // Backpressure: the socket is not draining fast enough.
                    // Skip ahead to the ring tail rather than growing forever.
                    if queued_bytes > SEND_QUEUE_HIGH_WATER || tx.capacity() == 0 {
                        let (start, tail) = session.read_since(next_seq);
                        let ev = ControlEvent::PtyResync {
                            session_id: session.id(),
                            from_seq: start,
                        };
                        let _ = tx
                            .send(Outbound::Text(serde_json::to_string(&ev).unwrap()))
                            .await;
                        next_seq = start + tail.len() as u64;
                        queued_bytes = 0;
                        let frame = BinaryFrame {
                            kind: FrameKind::PtyOutput,
                            handle,
                            seq: start,
                            payload: tail,
                        };
                        if tx.send(Outbound::Binary(frame.encode())).await.is_err() {
                            return;
                        }
                        continue;
                    }

                    let frame = BinaryFrame {
                        kind: FrameKind::PtyOutput,
                        handle,
                        seq: next_seq,
                        payload: chunk.to_vec(),
                    };
                    next_seq += chunk.len() as u64;
                    queued_bytes = queued_bytes.saturating_sub(chunk.len());
                    if tx.send(Outbound::Binary(frame.encode())).await.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Lagged(_)) => {
                    // The broadcast buffer overflowed. Resync from the ring.
                    let (start, tail) = session.read_since(next_seq);
                    let ev = ControlEvent::PtyResync {
                        session_id: session.id(),
                        from_seq: start,
                    };
                    let _ = tx
                        .send(Outbound::Text(serde_json::to_string(&ev).unwrap()))
                        .await;
                    next_seq = start + tail.len() as u64;
                    let frame = BinaryFrame {
                        kind: FrameKind::PtyOutput,
                        handle,
                        seq: start,
                        payload: tail,
                    };
                    if tx.send(Outbound::Binary(frame.encode())).await.is_err() {
                        return;
                    }
                }
                Err(tokio::sync::broadcast::error::RecvError::Closed) => {
                    let ev = ControlEvent::PtyExit {
                        session_id: session.id(),
                        status: 0,
                    };
                    let _ = tx
                        .send(Outbound::Text(serde_json::to_string(&ev).unwrap()))
                        .await;
                    return;
                }
            }
        }
    })
}
```

- [ ] **Step 4: Run the tests to verify they pass**

Run: `cargo test -p omnireach-host --test transport`
Expected: PASS, 2 tests.

- [ ] **Step 5: Verify Windows target still checks**

Run: `cargo check --all --target x86_64-pc-windows-msvc`
Expected: success.

- [ ] **Step 6: Commit**

```bash
git add crates/host/src/transport.rs crates/host/src/lib.rs crates/host/Cargo.toml crates/host/tests/transport.rs
git commit -m "feat: multiplexed websocket transport with backpressure resync"
```

---

### Task 10: Host binary wiring

**Files:**
- Replace: `crates/host/src/main.rs`

**Interfaces:**
- Consumes: everything from Tasks 6–9.
- Produces: a runnable `omnireach-host` binary.

- [ ] **Step 1: Write `main.rs`**

```rust
use omnireach_host::{auth::Authenticator, config::Config, sessions::SessionRegistry, transport};
use std::sync::Arc;

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    tracing_subscriber::fmt()
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env()
                .unwrap_or_else(|_| "omnireach_host=info".into()),
        )
        .init();

    let config = Arc::new(Config::load()?);
    let registry = Arc::new(SessionRegistry::new(config.scrollback_bytes));

    let auth = match &config.access {
        Some(a) => {
            tracing::info!("Cloudflare Access enabled for team {}", a.team_domain);
            Some(Arc::new(Authenticator::new(a.clone())))
        }
        None => {
            tracing::warn!(
                "Cloudflare Access is NOT configured — the host accepts unauthenticated \
                 websocket connections. Bind to localhost only."
            );
            None
        }
    };

    let state = transport::AppState {
        registry: Arc::clone(&registry),
        config: Arc::clone(&config),
        auth,
    };

    let listener = tokio::net::TcpListener::bind(&config.listen_addr).await?;
    tracing::info!("omnireach-host listening on {}", config.listen_addr);

    axum::serve(listener, transport::router(state)).await?;
    Ok(())
}
```

- [ ] **Step 2: Run the binary and confirm it starts**

Run: `cargo run -p omnireach-host` (then Ctrl-C)
Expected: logs `omnireach-host listening on 127.0.0.1:7717` and the Access warning.

- [ ] **Step 3: Confirm `/health` answers**

In a second shell, with the host running: `curl -s http://127.0.0.1:7717/health`
Expected: `{"status":"ok"}`

- [ ] **Step 4: Commit**

```bash
git add crates/host/src/main.rs
git commit -m "feat: host binary wiring config, registry, auth and transport"
```

---

### Task 11: The load-bearing replay test

This test **is** the specification for session persistence. If it does not exist, persistence is unverified no matter how good the feature looks by hand.

**Files:**
- Create: `crates/host/tests/replay.rs`

**Interfaces:**
- Consumes: the whole host stack.

- [ ] **Step 1: Write the test**

```rust
//! The load-bearing test for OmniReach: a client that disconnects mid-stream
//! and reattaches must observe a byte-identical stream to one that never
//! disconnected at all.

use futures_util::{SinkExt, StreamExt};
use omnireach_protocol::{BinaryFrame, FrameKind};
use std::sync::Arc;
use std::time::Duration;
use tokio_tungstenite::tungstenite::Message;

async fn start_host() -> String {
    let config = Arc::new(omnireach_host::config::Config::from_str("").unwrap());
    let registry = Arc::new(omnireach_host::sessions::SessionRegistry::new(1024 * 1024));
    let state = omnireach_host::transport::AppState { registry, config, auth: None };
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    tokio::spawn(async move {
        axum::serve(listener, omnireach_host::transport::router(state)).await.unwrap();
    });
    format!("ws://{addr}/ws")
}

/// Drain output frames for `dur`, returning (bytes, highest_seq_consumed).
async fn drain(
    ws: &mut tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    dur: Duration,
) -> (Vec<u8>, u64) {
    let mut out = Vec::new();
    let mut high = 0u64;
    let deadline = tokio::time::Instant::now() + dur;
    while tokio::time::Instant::now() < deadline {
        let Ok(Some(Ok(msg))) = tokio::time::timeout(Duration::from_millis(300), ws.next()).await
        else {
            continue;
        };
        if let Message::Binary(b) = msg {
            if let Ok(f) = BinaryFrame::decode(&b) {
                if f.kind == FrameKind::PtyOutput {
                    high = f.seq + f.payload.len() as u64;
                    out.extend_from_slice(&f.payload);
                }
            }
        }
    }
    (out, high)
}

#[tokio::test]
async fn reconnect_and_replay_produces_a_byte_identical_stream() {
    let url = start_host().await;

    // A child that emits 400 numbered lines slowly enough to span a disconnect.
    let script = "i=0; while [ $i -lt 400 ]; do printf 'line-%d\\n' $i; i=$((i+1)); sleep 0.005; done";

    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws.send(Message::Text(
        serde_json::json!({
            "id": 1, "op": "pty.create",
            "params": {"shell": null, "cwd": null, "cols": 80, "rows": 24}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let session_id = loop {
        if let Some(Ok(Message::Text(t))) = ws.next().await {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            if v["id"] == 1 {
                assert_eq!(v["ok"], true, "create failed: {v}");
                break v["result"]["session_id"].as_str().unwrap().to_string();
            }
        }
    };

    let handle = 1u32;
    ws.send(Message::Binary(
        BinaryFrame {
            kind: FrameKind::PtyInput,
            handle,
            seq: 0,
            payload: format!("{script}\n").into_bytes(),
        }
        .encode()
        .into(),
    ))
    .await
    .unwrap();

    // Consume part of the output, then hard-drop the connection mid-stream.
    let (first_half, last_seq) = drain(&mut ws, Duration::from_millis(700)).await;
    drop(ws);
    assert!(!first_half.is_empty(), "expected output before disconnect");

    // Stay disconnected while the child keeps producing.
    tokio::time::sleep(Duration::from_millis(800)).await;

    // Reattach from exactly where we left off.
    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws2.send(Message::Text(
        serde_json::json!({
            "id": 2, "op": "pty.attach",
            "params": {"session_id": session_id, "since": last_seq}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    loop {
        if let Some(Ok(Message::Text(t))) = ws2.next().await {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            if v["id"] == 2 {
                assert_eq!(v["ok"], true, "attach failed: {v}");
                assert!(
                    v["result"]["truncated_from"].is_null(),
                    "1MB ring should not have truncated this test's output"
                );
                break;
            }
        }
    }

    let (second_half, _) = drain(&mut ws2, Duration::from_secs(4)).await;

    let mut combined = first_half;
    combined.extend_from_slice(&second_half);
    let text = String::from_utf8_lossy(&combined);

    // Every line must appear exactly once, in order, with no gap and no dupe.
    for i in 0..400 {
        let needle = format!("line-{i}\n");
        assert_eq!(
            text.matches(&needle).count(),
            1,
            "line-{i} appeared {} times across the disconnect — replay is not \
             byte-exact (duplicate means the resume offset was too early, \
             missing means it was too late)",
            text.matches(&needle).count()
        );
    }
}

#[tokio::test]
async fn session_keeps_running_while_no_client_is_attached() {
    let url = start_host().await;
    let (mut ws, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws.send(Message::Text(
        serde_json::json!({
            "id": 1, "op": "pty.create",
            "params": {"shell": null, "cwd": null, "cols": 80, "rows": 24}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let session_id = loop {
        if let Some(Ok(Message::Text(t))) = ws.next().await {
            let v: serde_json::Value = serde_json::from_str(&t).unwrap();
            if v["id"] == 1 {
                break v["result"]["session_id"].as_str().unwrap().to_string();
            }
        }
    };

    ws.send(Message::Binary(
        BinaryFrame {
            kind: FrameKind::PtyInput,
            handle: 1,
            seq: 0,
            payload: b"sleep 0.5; printf 'produced-while-away'\n".to_vec(),
        }
        .encode()
        .into(),
    ))
    .await
    .unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    drop(ws); // leave before the output exists

    tokio::time::sleep(Duration::from_millis(1200)).await;

    let (mut ws2, _) = tokio_tungstenite::connect_async(&url).await.unwrap();
    ws2.send(Message::Text(
        serde_json::json!({
            "id": 2, "op": "pty.attach",
            "params": {"session_id": session_id, "since": 0}
        })
        .to_string()
        .into(),
    ))
    .await
    .unwrap();

    let (bytes, _) = drain(&mut ws2, Duration::from_secs(3)).await;
    assert!(
        String::from_utf8_lossy(&bytes).contains("produced-while-away"),
        "output produced while disconnected was lost"
    );
}
```

- [ ] **Step 2: Run it**

Run: `cargo test -p omnireach-host --test replay -- --nocapture`
Expected: PASS, 2 tests.

If `reconnect_and_replay_produces_a_byte_identical_stream` reports duplicates, the pump is replaying from before the client's true resume point — check that `next_seq` in `spawn_pump` advances by the backlog length. If it reports missing lines, the subscription is being created after the replay read rather than before.

- [ ] **Step 3: Run the full check script**

Run: `./scripts/check.sh`
Expected: fmt clean, clippy clean, all tests pass, Windows target checks.

- [ ] **Step 4: Commit**

```bash
git add crates/host/tests/replay.rs
git commit -m "test: byte-identical reconnect replay, the persistence specification"
```

---

### Task 12: Tauri client scaffold

**Files:**
- Create: `client/` via `npm create tauri-app`
- Create: `client/src/main.ts`, `client/src/style.css`, `client/index.html`

**Interfaces:**
- Produces: a macOS app window rendering one xterm.js terminal wired to the host over WebSocket.

**Note.** This task requires network access for `npm install` and a first `cargo build` of Tauri's dependency tree, which is large. Budget time accordingly.

- [ ] **Step 1: Scaffold the app**

```bash
cd client 2>/dev/null || mkdir -p client
npm create tauri-app@latest . -- --template vanilla-ts --manager npm --yes
npm install
npm install @xterm/xterm @xterm/addon-fit
```

- [ ] **Step 2: Verify it builds and runs**

Run: `npm run tauri dev`
Expected: a native macOS window opens. Close it.

- [ ] **Step 3: Replace `client/index.html`**

```html
<!doctype html>
<html lang="en">
  <head>
    <meta charset="UTF-8" />
    <title>OmniReach</title>
    <link rel="stylesheet" href="/node_modules/@xterm/xterm/css/xterm.css" />
    <link rel="stylesheet" href="/src/style.css" />
  </head>
  <body>
    <div id="status" class="status disconnected">disconnected</div>
    <div id="terminal"></div>
    <script type="module" src="/src/main.ts"></script>
  </body>
</html>
```

- [ ] **Step 4: Create `client/src/style.css`**

```css
:root { color-scheme: dark; }
* { box-sizing: border-box; }
body {
  margin: 0;
  height: 100vh;
  display: flex;
  flex-direction: column;
  background: #10100f;
  font: 13px ui-sans-serif, system-ui, sans-serif;
  color: #e8e6e3;
}
.status {
  padding: 6px 12px;
  font-variant-numeric: tabular-nums;
  border-bottom: 1px solid #2a2a28;
}
.status.connected    { color: #7dcfa0; }
.status.reconnecting { color: #e6c07b; }
.status.disconnected { color: #e88b8b; }
.status.auth-expired { color: #e88b8b; }
#terminal { flex: 1; min-height: 0; padding: 8px; }
```

- [ ] **Step 5: Commit**

```bash
git add client
git commit -m "feat: tauri client scaffold with xterm.js"
```

---

### Task 13: Client connection core with reconnect

**Files:**
- Create: `client/src/connection.ts`
- Create: `client/src/frame.ts`

**Interfaces:**
- Consumes: the wire protocol from Tasks 3–4.
- Produces:
  - `encodeFrame(kind, handle, seq, payload): Uint8Array`
  - `decodeFrame(buf: ArrayBuffer): {kind, handle, seq, payload}`
  - `class Connection` with `connect()`, `send(op, params): Promise<any>`, `onOutput(cb)`, `onStatus(cb)`, `onResync(cb)`

**Reconnect policy (from the spec):** base 250 ms, ×1.8, cap 15 s, ±20% jitter.

- [ ] **Step 1: Create `client/src/frame.ts`**

```ts
export const HEADER_LEN = 13;

export const FrameKind = {
  PtyOutput: 0x01,
  PtyInput: 0x02,
  FsChunk: 0x03,
} as const;

export interface Frame {
  kind: number;
  handle: number;
  seq: bigint;
  payload: Uint8Array;
}

export function encodeFrame(
  kind: number,
  handle: number,
  seq: bigint,
  payload: Uint8Array,
): Uint8Array {
  const out = new Uint8Array(HEADER_LEN + payload.length);
  const view = new DataView(out.buffer);
  out[0] = kind;
  view.setUint32(1, handle, true);
  view.setBigUint64(5, seq, true);
  out.set(payload, HEADER_LEN);
  return out;
}

export function decodeFrame(buf: ArrayBuffer): Frame {
  if (buf.byteLength < HEADER_LEN) {
    throw new Error(`frame too short: ${buf.byteLength}`);
  }
  const view = new DataView(buf);
  return {
    kind: view.getUint8(0),
    handle: view.getUint32(1, true),
    seq: view.getBigUint64(5, true),
    payload: new Uint8Array(buf, HEADER_LEN),
  };
}
```

- [ ] **Step 2: Create `client/src/connection.ts`**

```ts
import { decodeFrame, encodeFrame, FrameKind } from "./frame";

export type Status =
  | "connected"
  | "reconnecting"
  | "disconnected"
  | "auth-expired";

interface Pending {
  resolve: (v: any) => void;
  reject: (e: Error) => void;
}

const BASE_DELAY_MS = 250;
const MAX_DELAY_MS = 15_000;
const FACTOR = 1.8;
const JITTER = 0.2;

export class Connection {
  private ws: WebSocket | null = null;
  private nextId = 1;
  private pending = new Map<number, Pending>();
  private attempt = 0;
  private closedByUser = false;

  private outputCb: (handle: number, seq: bigint, data: Uint8Array) => void =
    () => {};
  private statusCb: (s: Status) => void = () => {};
  private resyncCb: (sessionId: string, fromSeq: bigint) => void = () => {};
  private openCb: () => void = () => {};

  constructor(private url: string) {}

  onOutput(cb: typeof this.outputCb) { this.outputCb = cb; }
  onStatus(cb: typeof this.statusCb) { this.statusCb = cb; }
  onResync(cb: typeof this.resyncCb) { this.resyncCb = cb; }
  onOpen(cb: () => void) { this.openCb = cb; }

  connect(): void {
    this.closedByUser = false;
    this.open();
  }

  close(): void {
    this.closedByUser = true;
    this.ws?.close();
    this.statusCb("disconnected");
  }

  private open(): void {
    this.statusCb(this.attempt === 0 ? "reconnecting" : "reconnecting");
    const ws = new WebSocket(this.url);
    ws.binaryType = "arraybuffer";
    this.ws = ws;

    ws.onopen = () => {
      this.attempt = 0;
      this.statusCb("connected");
      this.openCb();
    };

    ws.onmessage = (ev) => {
      if (typeof ev.data === "string") {
        this.handleControl(JSON.parse(ev.data));
      } else {
        const f = decodeFrame(ev.data as ArrayBuffer);
        if (f.kind === FrameKind.PtyOutput) {
          this.outputCb(f.handle, f.seq, f.payload);
        }
      }
    };

    ws.onclose = () => {
      // Reject everything in flight — a pending promise that never settles
      // is a silently hung UI.
      for (const [, p] of this.pending) {
        p.reject(new Error("connection closed"));
      }
      this.pending.clear();
      if (!this.closedByUser) this.scheduleReconnect();
      else this.statusCb("disconnected");
    };

    ws.onerror = () => { /* onclose always follows; handle it there */ };
  }

  private scheduleReconnect(): void {
    this.statusCb("reconnecting");
    const raw = Math.min(
      BASE_DELAY_MS * Math.pow(FACTOR, this.attempt),
      MAX_DELAY_MS,
    );
    const jitter = raw * JITTER * (Math.random() * 2 - 1);
    this.attempt += 1;
    setTimeout(() => this.open(), Math.max(0, raw + jitter));
  }

  private handleControl(msg: any): void {
    if (typeof msg.id === "number") {
      const p = this.pending.get(msg.id);
      if (!p) return;
      this.pending.delete(msg.id);
      if (msg.ok) p.resolve(msg.result);
      else p.reject(new Error(`${msg.error?.code}: ${msg.error?.message}`));
      return;
    }
    if (msg.event === "pty.resync") {
      this.resyncCb(msg.session_id, BigInt(msg.from_seq));
    }
  }

  send(op: string, params: unknown): Promise<any> {
    return new Promise((resolve, reject) => {
      if (this.ws?.readyState !== WebSocket.OPEN) {
        reject(new Error("not connected"));
        return;
      }
      const id = this.nextId++;
      this.pending.set(id, { resolve, reject });
      this.ws.send(JSON.stringify({ id, op, params }));
    });
  }

  sendInput(handle: number, data: string): void {
    if (this.ws?.readyState !== WebSocket.OPEN) return;
    const payload = new TextEncoder().encode(data);
    this.ws.send(encodeFrame(FrameKind.PtyInput, handle, 0n, payload));
  }
}
```

- [ ] **Step 3: Verify it type-checks**

Run: `cd client && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 4: Commit**

```bash
git add client/src/frame.ts client/src/connection.ts
git commit -m "feat: client connection core with reconnect and backoff"
```

---

### Task 14: Terminal pane wired end to end

**Files:**
- Create/replace: `client/src/main.ts`

**Interfaces:**
- Consumes: `Connection` (Task 13), the host from Tasks 9–10.
- Produces: a working terminal against a locally running host.

**Renderer note.** Use xterm.js's **default canvas renderer**, not the WebGL addon. Tauri renders in WKWebView, where the WebGL path is the less-travelled one for high-throughput terminals (Global Constraints).

- [ ] **Step 1: Write `client/src/main.ts`**

```ts
import { Terminal } from "@xterm/xterm";
import { FitAddon } from "@xterm/addon-fit";
import { Connection, type Status } from "./connection";

const HOST_URL = import.meta.env.VITE_HOST_URL ?? "ws://127.0.0.1:7717/ws";
const RESUME_KEY = "omnireach.session";

interface Resume {
  sessionId: string;
  lastSeq: string; // BigInt is not JSON-serializable
}

const statusEl = document.getElementById("status") as HTMLDivElement;
const termEl = document.getElementById("terminal") as HTMLDivElement;

const term = new Terminal({
  fontFamily: "ui-monospace, SFMono-Regular, Menlo, monospace",
  fontSize: 13,
  theme: { background: "#10100f", foreground: "#e8e6e3" },
  scrollback: 20_000,
});
const fit = new FitAddon();
term.loadAddon(fit);
term.open(termEl);
fit.fit();

const decoder = new TextDecoder();
let handle: number | null = null;
let lastSeq = 0n;

function setStatus(s: Status) {
  statusEl.textContent = s;
  statusEl.className = `status ${s}`;
}

function loadResume(): Resume | null {
  const raw = localStorage.getItem(RESUME_KEY);
  return raw ? (JSON.parse(raw) as Resume) : null;
}

function saveResume(sessionId: string) {
  localStorage.setItem(
    RESUME_KEY,
    JSON.stringify({ sessionId, lastSeq: lastSeq.toString() } satisfies Resume),
  );
}

const conn = new Connection(HOST_URL);

conn.onStatus(setStatus);

conn.onOutput((h, seq, data) => {
  if (h !== handle) return;
  term.write(decoder.decode(data, { stream: true }));
  lastSeq = seq + BigInt(data.length);
});

conn.onResync((_sessionId, fromSeq) => {
  // Do NOT clear: client-side scrollback above this point is still valid.
  term.write("\r\n\x1b[2m— resynced, some output skipped —\x1b[0m\r\n");
  lastSeq = fromSeq;
});

conn.onOpen(async () => {
  const resume = loadResume();
  try {
    if (resume) {
      const r = await conn.send("pty.attach", {
        session_id: resume.sessionId,
        since: Number(BigInt(resume.lastSeq)),
      });
      handle = r.handle;
      lastSeq = BigInt(resume.lastSeq);
      if (r.truncated_from != null) {
        term.write("\r\n\x1b[2m— output lost —\x1b[0m\r\n");
        lastSeq = BigInt(r.truncated_from);
      }
      return;
    }
  } catch {
    // Session is gone (host restarted). Fall through and make a new one.
    localStorage.removeItem(RESUME_KEY);
    term.write("\r\n\x1b[2m— previous session unavailable, starting a new one —\x1b[0m\r\n");
  }

  const r = await conn.send("pty.create", {
    shell: null,
    cwd: null,
    cols: term.cols,
    rows: term.rows,
  });
  handle = r.handle;
  lastSeq = 0n;
  saveResume(r.session_id);
});

term.onData((d) => {
  if (handle != null) conn.sendInput(handle, d);
});

term.onResize(({ cols, rows }) => {
  if (handle != null) conn.send("pty.resize", { handle, cols, rows }).catch(() => {});
});

window.addEventListener("resize", () => fit.fit());
setInterval(() => {
  const r = loadResume();
  if (r) saveResume(r.sessionId);
}, 2000);

setStatus("disconnected");
conn.connect();
```

- [ ] **Step 2: Type-check**

Run: `cd client && npx tsc --noEmit`
Expected: no errors.

- [ ] **Step 3: Manual end-to-end verification**

In one shell: `cargo run -p omnireach-host`
In another: `cd client && npm run tauri dev`

Verify by hand, and record the result honestly:
1. The window opens, status reads **connected**, and a shell prompt appears.
2. Typing `ls` and pressing Return produces output.
3. Run `for i in $(seq 1 200); do echo "line $i"; sleep 0.05; done`, then **quit the client app entirely** mid-run.
4. Relaunch the client. The missed lines appear, in order, with no duplicates, and the loop is still running.

Step 4 is the whole product. If it fails, fix it before proceeding.

- [ ] **Step 4: Commit**

```bash
git add client/src/main.ts
git commit -m "feat: terminal pane with session resume across app restarts"
```

---

## What this plan does NOT deliver

Stated plainly so it is not mistaken for complete:

- **No Windows verification.** All host code is type-checked for `x86_64-pc-windows-msvc` but executed only against Unix PTYs on macOS. ConPTY, `pwsh.exe`, and the shipped `.exe` are unverified. A human must run Tasks 5, 9, 10 and 14's manual check on real Windows hardware.
- **No Cloudflare Tunnel setup.** Task 8 validates Access JWTs, but nothing configures `cloudflared`, creates the named tunnel, or implements the client's SSO login flow (spec §6.2). That flow's endpoint shape must be verified against live Cloudflare docs — see the spec's warning. This is the remainder of M3.
- **Single terminal in the client.** The host supports unlimited concurrent sessions (`pty.list`, per-handle multiplexing all work); the client UI drives one. Multi-pane UI is the remainder of M4.
- **No file browser (M5) and no remote browser (M6/M7).** Separate specs.
