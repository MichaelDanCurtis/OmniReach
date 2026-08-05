# OmniReach

Drive CLI tools — primarily Claude Code — running on a Windows machine, from a
native macOS client, over a Cloudflare tunnel. The useful part of RDP without
streaming a desktop.

The defining requirement is **session survival**: an agent task started from a
laptop keeps running when that laptop closes, and is fully reviewable when you
reconnect an hour later from somewhere else.

- Design: [`docs/superpowers/specs/2026-07-22-omnireach-terminal-core-design.md`](docs/superpowers/specs/2026-07-22-omnireach-terminal-core-design.md)
- Plan: [`docs/superpowers/plans/2026-07-22-omnireach-terminal-core.md`](docs/superpowers/plans/2026-07-22-omnireach-terminal-core.md)

## Layout

```
crates/protocol/   wire format — binary frame codec + JSON control types
crates/host/       the host: ring, PTY sessions, registry, auth, transport
client/            Tauri v2 + xterm.js macOS client
scripts/check.sh   fmt + clippy + tests + Windows type-check
```

## Running it locally

Two terminals:

```bash
cargo run -p omnireach-host
```

```bash
cd client && npm install && npm run tauri dev
```

The host listens on `127.0.0.1:7717` and logs a warning that Cloudflare Access
is not configured — that is correct for local use, and the reason it must stay
bound to localhost until it is.

## Checks

```bash
./scripts/check.sh
```

Runs fmt, clippy (`-D warnings`), the full test suite, and the Windows
type-check.

## Configuration

`~/.config/OmniReach/config.toml` on Unix, `%APPDATA%\OmniReach\config.toml` on
Windows. Absent, sensible defaults apply.

```toml
listen_addr      = "127.0.0.1:7717"
scrollback_bytes = 2_097_152

[access]
team_domain = "myteam.cloudflareaccess.com"
aud         = "<application audience tag>"

[[shells]]
name    = "pwsh"
command = "powershell.exe"
args    = ["-NoLogo"]
default = true
```

## State of the work

Milestones M1–M4 of the plan, less the Cloudflare tunnel setup. **57 tests
passing.**

Built and verified:

- Byte-transparent scrollback ring with monotonic sequence numbers
- PTY sessions owned by the host, independent of any connection
- Session registry, with finished sessions retained for an hour so a late
  reconnect can still read what happened
- Multiplexed WebSocket transport with real backpressure
- Cloudflare Access JWT validation (JWKS cached, refreshed on unknown `kid`)
- `pty.exit` carries the child's real wait status, not a placeholder
- A Tauri + xterm.js client with dynamic terminals: 4 spawn on first launch,
  tabs to switch, [+]/[×] to open and close. Sessions resume across app
  restarts by replaying the host's ring from 0, so scrollback is repainted
  rather than the pane coming back blank; live reconnects resume at
  `last_seq` so nothing duplicates. Each pane owns its `TextDecoder` —
  a shared stateful decoder would corrupt multi-byte sequences when
  interleaved frames from different panes split them.

### Not verified — read before trusting any of it

- **Nothing Windows-specific has ever been executed.** All host code
  type-checks for `x86_64-pc-windows-msvc`, but development and every test ran
  on macOS against Unix PTYs. ConPTY behaviour, `pwsh.exe` spawning, and the
  produced `.exe` are unproven. Specifically unknown: whether dropping the
  slave produces EOF on a ConPTY master, and whether `kill()` unblocks a
  ConPTY read. Both are load-bearing.
- **No Cloudflare tunnel setup exists.** JWT validation works; nothing
  configures `cloudflared`, creates the named tunnel, or implements the
  client's SSO login. That flow's endpoint shape must be checked against live
  Cloudflare docs rather than trusted from the spec.
- **The client's Enter-key path is untested.** Multi-terminal routing, resume,
  close/switch, and exit reporting were all verified headlessly through the
  app's real send path (`conn.sendInput`), and the host is confirmed to
  execute on a bare CR — but no keydown event reaches xterm in a headless
  browser, so the one remaining unexercised link is xterm's own
  `Enter → onData('\r')` mapping.

### Not built

File browser (M5) and the remote headless-Chrome session (M6/M7). Each needs
its own spec; the protocol reserves frame-kind space for both.

## Traps worth knowing before editing

Both of these make a **failure look like a hang**, which is why they are
documented in the code as well as here.

1. `Runtime::drop` waits for `spawn_blocking` tasks and cannot cancel them.
   Every PTY read loop is one. A test that leaves a live shell running hangs
   teardown forever — and if it was already panicking, the panic message is
   swallowed. Kill sessions **before** asserting.
2. A PTY applies ONLCR: the shell writes `\n`, the client receives `\r\n`.
   Assertions on `\n` silently never match.

And one that fails loudly but confusingly: `jsonwebtoken` requires an explicit
crypto backend feature. Without one it compiles and panics at runtime.
`rust_crypto` is used rather than `aws_lc_rs` because aws-lc builds C requiring
`windows.h`, which breaks the Windows cross-check.
