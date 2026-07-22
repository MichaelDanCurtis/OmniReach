#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all

# Windows type-check. This is the ONLY signal we get that the ConPTY code path
# compiles, since no Windows machine is available here.
#
# --no-default-features excludes the `access-auth` feature. That is not laziness:
# its TLS stack (rustls -> aws-lc-rs / ring) compiles C requiring windows.h, so
# a cross-target `cargo check` fails in the build script before ever reaching
# our code. Nothing in auth.rs is platform-specific, so no Windows coverage is
# lost. Everything that touches ConPTY (pty.rs, sessions.rs, transport.rs) IS
# covered here.
echo "--- windows target type-check (sans access-auth; see comment) ---"
cargo check -p omnireach-protocol --target x86_64-pc-windows-msvc
cargo check -p omnireach-host --no-default-features --target x86_64-pc-windows-msvc
