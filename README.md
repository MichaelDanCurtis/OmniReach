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
crates/host/       ring, PTY sessions, registry, filesystem, browser,
                   auth, transport
client/            Tauri v2 client — terminals, file browser, remote browser
scripts/check.sh   fmt + clippy + tests + Windows type-check
```

## What it does

Three views, one multiplexed WebSocket:

- **Terminals** — persistent PTY sessions. Four spawn on first launch; open
  and close more freely. Close the laptop mid-run and reconnect later: the
  session is still running and its scrollback is replayed.
- **Files** — browse the host filesystem, upload and download with 1 MB
  chunking and resume-by-offset.
- **Browser** — a headless Chrome running on the host, streamed as JPEG
  frames, with real mouse and keyboard input, tabs, a JavaScript console,
  and downloads routed where the file browser can see them.

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

Milestones M1–M7 less the Cloudflare tunnel setup. **~120 tests passing.**

Built and verified:

- Byte-transparent scrollback ring with monotonic sequence numbers
- PTY sessions owned by the host, independent of any connection
- Session registry, with finished sessions retained for an hour so a late
  reconnect can still read what happened
- Multiplexed WebSocket transport with real backpressure
- Cloudflare Access JWT validation (JWKS cached, refreshed on unknown `kid`)
- `pty.exit` carries the child's real wait status, not a placeholder
- Whole-filesystem browse and transfer, byte-transparent and resumable.
  `remove` is deliberately non-recursive: deleting a tree by accident from a
  browse UI is unrecoverable.
- A remote headless Chrome: tabs, navigation, JPEG screencast, mouse and
  keyboard input, a JS console, and downloads. Click mapping is verified
  against a negative control — a click aimed away from the target must miss.
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
- **No Cloudflare tunnel setup exists, and the client cannot authenticate
  through Access as currently built.** Two separate facts:

  1. Host-side validation is *correct* — checked against live Cloudflare docs
     on 2026-08-05: the origin receives the token in the
     `Cf-Access-Jwt-Assertion` header, and the JWKS lives at
     `https://<team>.cloudflareaccess.com/cdn-cgi/access/certs`. Both match
     what `auth.rs` implements.
  2. But the client opens its WebSocket from the **webview**, and the browser
     WebSocket API cannot set request headers — so it cannot present that
     header at all. The design called for a Rust core owning the WS connection
     precisely to avoid this; the implementation drifted. Fixing it means
     moving the socket into `src-tauri` and bridging frames to the webview.
     The common workaround — passing the JWT as a URL query parameter — is
     rejected here: it leaks a credential into proxy and server logs.

  Until that is done, the host must stay bound to localhost or a trusted LAN.
  Nothing configures `cloudflared` or creates the named tunnel either.
- **The client's Enter-key path is untested.** Multi-terminal routing, resume,
  close/switch, and exit reporting were all verified headlessly through the
  app's real send path (`conn.sendInput`), and the host is confirmed to
  execute on a bare CR — but no keydown event reaches xterm in a headless
  browser, so the one remaining unexercised link is xterm's own
  `Enter → onData('\r')` mapping.

### Not built

Nothing from the original scope remains unbuilt. The outstanding work is the
Cloudflare tunnel setup and the client-auth transport gap described above.

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
