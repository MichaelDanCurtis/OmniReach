# OmniReach — Terminal Core Design

**Date:** 2026-07-22
**Scope:** Milestones M1–M4 (host, protocol, persistent terminals, Cloudflare Access)
**Status:** Approved for planning

---

## 1. Purpose

OmniReach lets a developer drive CLI tools — primarily Claude Code — running on a Windows
machine, from a native macOS client, over a Cloudflare Tunnel. It aims to deliver the useful
part of RDP (a live shell on a remote box) without the cost of streaming a desktop.

The defining requirement is **session survival**: an agent task started from a laptop must keep
running when that laptop closes, and must be fully reviewable when the user reconnects an hour
later from somewhere else. Everything else in the system is secondary to that.

### Out of scope for this spec

Deferred to their own specs, because neither can be planned well until the wire protocol has
survived contact with M2:

- **M5** — file browser with upload/download (whole-filesystem access, chunked transfer)
- **M6/M7** — remote headless-Chrome session (screencast, input, tabs, console, downloads)

The protocol defined here reserves channel space for both so they can be added without a
breaking change.

---

## 2. Decisions

| Decision | Choice | Rationale |
|---|---|---|
| Client | Native macOS app (Tauri v2) | Native window management, OS shortcuts, local notifications |
| Language | Rust, host and client | Single 8 MB host `.exe` with no runtime — matters for installing on a second machine |
| Auth | Cloudflare Access + browser SSO | Real identity and audit logs; edge rejects unauthenticated traffic |
| Sessions | Persistent with scrollback replay | The core value proposition |
| Terminal count | Dynamic | 4 is a launch default, not a constraint |
| Filesystem scope | Whole filesystem | Terminals already grant equivalent reach; a roots allowlist would be theater over an open shell |
| Default shell | PowerShell, configurable per terminal | Matches Windows dev norms without locking out WSL/Git Bash |

### Accepted risks

These were chosen with the trade-off stated, and are recorded so they are not re-litigated:

1. **`chromiumoxide` is thinner than Playwright** for screencast work (M6+). Mitigation: drive
   CDP directly where the wrapper is inadequate.
2. **Tauri on macOS uses WKWebView**, not Chromium. xterm.js works there, but the WebGL renderer
   is the less-travelled path for high-throughput terminals. Ship with the **canvas** renderer;
   only move to WebGL if measurement justifies it.
3. **Rust costs more development time than TypeScript** for the same v1. Bought deliberately in
   exchange for the install story.

---

## 3. Architecture

```
┌─ Windows host — omnireach-host.exe ─────────────────────────┐
│  axum listener, 127.0.0.1:7717                              │
│  ├─ auth      validate CF Access JWT (JWKS, cached)         │
│  ├─ sessions  PTY registry — owns PTYs, not connections     │
│  ├─ fs        [M5]                                          │
│  ├─ browser   [M6/M7]                                       │
│  └─ config    %APPDATA%\OmniReach\config.toml               │
└────────────────────────┬────────────────────────────────────┘
                  cloudflared (named tunnel)
                         ↓
            Cloudflare edge — Access policy (SSO)
                         ↓
┌─ macOS client — OmniReach.app (Tauri v2) ───────────────────┐
│  Rust core   SSO login → localhost callback → JWT → Keychain│
│              WS client, reconnect w/ backoff + jitter       │
│  WebView     xterm.js panes (canvas renderer)               │
└─────────────────────────────────────────────────────────────┘
```

### Component boundaries

Each unit below is independently testable and depends only on the ones above it.

| Unit | Responsibility | Depends on |
|---|---|---|
| `ring` | Bounded byte ring with monotonic sequence numbers | — |
| `protocol` | Frame encode/decode, control message types | — |
| `pty` | Wrap `portable_pty` child; own its ring; broadcast output | `ring` |
| `sessions` | Registry of `PtySession`; create/attach/detach/resize/kill | `pty` |
| `auth` | JWKS fetch + cache; validate Access JWT | — |
| `transport` | axum WS upgrade, per-connection multiplex, backpressure | `protocol`, `sessions`, `auth` |
| `client-core` | SSO login, token storage, WS client, reconnect state machine | `protocol` |
| `client-ui` | xterm.js panes, connection-state display | `client-core` |

The host process must run in the **interactive user session**, not as a Windows service.
Services run in session 0, where ConPTY and (later) Chrome behave badly. Autostart via a
Startup shortcut or a Task Scheduler at-logon task.

---

## 4. Wire protocol

A **single multiplexed WebSocket**. Access validation and reconnect logic are then written
exactly once, and Cloudflare's per-connection overhead is paid once.

### 4.1 Two message kinds

- **WebSocket text** → JSON control frames
- **WebSocket binary** → bulk data

Bulk data is raw bytes, never base64-in-JSON. Base64 would cost ~33% on the hottest path in
the system.

### 4.2 Binary frame layout

```
offset  size  field
0       1     kind    0x01 PtyOutput · 0x02 PtyInput · 0x03 FsChunk [M5]
1       4     handle  u32 LE — per-connection session handle
5       8     seq     u64 LE — for PtyOutput: byte offset of first payload byte
13      ..    payload
```

`handle` is assigned per connection at create/attach time. It is *not* the session id — session
ids are UUIDs and stable across reconnects; handles are small integers scoped to one connection.

### 4.3 Control frames

Request / response, correlated by `id`:

```json
{"id": 42, "op": "pty.create", "params": {"shell": "pwsh", "cwd": "C:\\dev", "cols": 120, "rows": 40}}
{"id": 42, "ok": true,  "result": {"session_id": "0f3c…", "handle": 1}}
{"id": 42, "ok": false, "error": {"code": "SHELL_NOT_FOUND", "message": "pwsh.exe not on PATH"}}
```

Unsolicited events carry no `id`:

```json
{"event": "pty.exit",   "session_id": "0f3c…", "status": 0}
{"event": "pty.resync", "session_id": "0f3c…", "from_seq": 918_233}
```

**Operations (M1–M4):**

| Op | Params | Result |
|---|---|---|
| `pty.list` | — | `[{session_id, shell, cwd, cols, rows, alive, head_seq}]` |
| `pty.create` | `shell, cwd, cols, rows` | `{session_id, handle}` |
| `pty.attach` | `session_id, since` | `{handle, cols, rows, truncated_from}` |
| `pty.detach` | `handle` | — |
| `pty.resize` | `handle, cols, rows` | — |
| `pty.kill` | `session_id` | — |

---

## 5. Session persistence

The crux of the system.

### 5.1 Ring buffer

Each `PtySession` owns a fixed-capacity byte ring (2 MB default, configurable) plus a monotonic
`head_seq` counting every byte the PTY has ever produced.

```rust
fn write(&mut self, bytes: &[u8]);              // appends, advances head_seq
fn oldest_seq(&self) -> u64;                    // head_seq - min(head_seq, capacity)
fn read_since(&self, seq: u64) -> (u64, Vec<u8>); // (actual_start_seq, bytes)
```

Backed by a fixed `Box<[u8]>` with wraparound cursors — not a `VecDeque` — to avoid allocation
churn on a hot path.

**The ring stores bytes, never strings.** Slicing a UTF-8 sequence at a chunk boundary and
lossy-decoding it produces replacement characters that permanently corrupt the pane, and the
corruption is baked into the buffer where no client can recover from it. xterm.js's parser
handles split sequences correctly across writes, so the host must never decode PTY output.
Nothing in the read or write path may call `String::from_utf8_lossy`.

### 5.2 Attach and replay

```
client → {"op": "pty.attach", "params": {"session_id": "S", "since": 12345}}
host   → {"ok": true, "result": {"handle": 1, "cols": 120, "rows": 40, "truncated_from": null}}
host   → binary PtyOutput frames starting at seq 12345
```

If `since` has already fallen out of the ring, the host sets `truncated_from` to `oldest_seq`
and replays from there. The client renders a visible marker:

```
— output lost —
```

A silent gap in a Claude Code transcript is materially worse than an honest one: the user would
have no way to know that the reasoning they are reading is missing a step.

### 5.3 Reconnect

The client persists `{session_id, last_seq}` per terminal **to disk**, so an app restart resumes
identically to a network blip.

Backoff: 250 ms base, ×1.8, capped at 15 s, ±20% jitter. Before each attempt the client checks
the stored JWT's `exp`; if expired it re-runs the SSO flow first (silent if Cloudflare's session
cookie is still valid, otherwise a browser window).

### 5.4 Backpressure

A PTY outruns a tunnel trivially — `cat` a large file and the producer is orders of magnitude
faster than the consumer. Unbounded queueing here is an OOM.

Per-connection send queue, 4 MB high-water mark. On exceeding it:

1. Drop queued `PtyOutput` frames for the offending session.
2. Emit `{"event": "pty.resync", "from_seq": N}`.
3. Send the ring's tail as a single frame.

The client writes a dim `— resynced, some output skipped —` line and then the snapshot,
**without clearing** the pane — clearing would destroy client-side scrollback that is still
perfectly good. A visible jump beats a stalled pane or a dead host.

### 5.5 Lifetime

Sessions are in-memory and **do not survive a host restart**. After a host reboot the client
shows previously-known sessions as dead and offers to respawn them with the same shell and
working directory. Persisting PTY state across reboots is not possible without process
migration and is explicitly not attempted.

---

## 6. Authentication

### 6.1 Host-side validation

- Fetch JWKS from `https://<team>.cloudflareaccess.com/cdn-cgi/access/certs`; cache, refresh on
  unknown `kid`, minimum refresh interval 5 minutes.
- Validate RS256 signature, `aud` equals the configured Application Audience tag, `iss` equals
  the team domain, and `exp` / `iat` / `nbf`.
- Token is read from the `Cf-Access-Jwt-Assertion` header. The client is a native WebSocket
  client (`tokio-tungstenite`) and can set request headers, so the cookie path is unnecessary.
- Validation happens **on the WebSocket upgrade request**. Because every reconnect is a fresh
  upgrade, this also provides natural re-validation over time.

### 6.2 Client-side SSO

Mirrors what `cloudflared access login` does:

1. Bind an ephemeral `127.0.0.1` listener.
2. Open the system browser to the Access CLI login endpoint for the host's hostname, passing
   that listener as the callback.
3. Receive the JWT on the callback, store it in the macOS Keychain.

⚠️ **Verify against current Cloudflare documentation during M3.** This flow is documented but
its exact endpoint shape has changed before, and this spec should not be trusted over the live
docs. Budget time in M3 for confirming it rather than assuming.

### 6.3 Configuration

`%APPDATA%\OmniReach\config.toml`:

```toml
listen_addr      = "127.0.0.1:7717"
scrollback_bytes = 2_097_152

[access]
team_domain = "myteam.cloudflareaccess.com"
aud         = "<application audience tag>"

[[shells]]
name    = "pwsh"
command = "pwsh.exe"
args    = ["-NoLogo"]
default = true

[[shells]]
name    = "wsl"
command = "wsl.exe"
args    = []
```

Note that WSL shells report POSIX paths (`/home/you`), which do not correspond to the Windows
paths the M5 file browser will use. That mismatch is a known source of confusion and must be
handled explicitly when M5 is specified.

---

## 7. Error handling

- The host never panics a connection away. Each session task carries a panic hook so one bad
  PTY cannot take down the process; all service functions return typed errors.
- Connection state is a **first-class UI element** in the client, with four visible states:
  connected · reconnecting · auth expired · host unreachable. A stalled pane that looks
  identical to an idle one is the single worst failure mode this system can have, so the state
  is always shown, never inferred.
- Host writes a rolling log to `%APPDATA%\OmniReach\logs\`.
- `/health` endpoint behind Access, for diagnosing whether the tunnel or the host is at fault.

---

## 8. Testing

**The load-bearing test:** start a process producing known output, kill the connection
mid-stream, reattach with the last received `seq`, and assert the concatenated result is
**byte-identical** to an uninterrupted run. This test is the specification for §5 — if it
passes, persistence works; if it does not exist, persistence is unverified regardless of how
the feature looks by hand.

Supporting coverage:

| Area | Test |
|---|---|
| `ring` | Property test: for arbitrary write sizes, `read_since(n)` always yields a suffix of everything written |
| `ring` | UTF-8: multi-byte sequences split across writes survive a replay byte-identically |
| `protocol` | Codec round-trip, including boundary values for `handle` and `seq` |
| `pty` | Spawn a real PTY (`cmd /c echo`), assert byte-exact output |
| `sessions` | Detach and reattach preserves the child process and its seq counter |
| `auth` | Reject: bad signature · wrong `aud` · wrong `iss` · expired · absent |
| `transport` | Backpressure: a fast producer triggers resync rather than unbounded growth |
| client | Playwright against the Tauri webview for the terminal pane |

---

## 9. Milestones

| | Deliverable |
|---|---|
| **M1** | Host skeleton, protocol, one PTY, localhost only. Client renders it. |
| **M2** | Persistence, reconnect, replay. *The hard part.* |
| **M3** | Named tunnel, Access JWT validation, client SSO. |
| **M4** | Dynamic terminals, per-terminal shell + cwd. |
| *M5* | *File browser, upload/download — separate spec* |
| *M6* | *Remote browser: single tab, screencast, input — separate spec* |
| *M7* | *Browser tabs, console, downloads — separate spec* |

M1–M4 constitute a shippable product: remote Claude Code that survives a closed laptop.
