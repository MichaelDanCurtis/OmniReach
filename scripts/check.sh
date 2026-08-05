#!/usr/bin/env bash
set -euo pipefail

cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
cargo test --all

# Windows type-check. This is the ONLY signal we get that the ConPTY code path
# compiles, since no Windows machine is available here.
#
# `access-auth` is excluded: its TLS stack (rustls -> aws-lc-rs / ring) compiles
# C requiring windows.h, so a cross-target `cargo check` fails inside a build
# script before reaching our code. `remote-browser` IS included — chromiumoxide
# without its `fetcher` feature pulls no TLS, so it cross-checks fine.
# Nothing in auth.rs is platform-specific, so no Windows coverage is lost.
echo "--- windows target type-check (sans access-auth; see comment) ---"
cargo check -p omnireach-protocol --target x86_64-pc-windows-msvc
cargo check -p omnireach-host --no-default-features --features remote-browser \
    --target x86_64-pc-windows-msvc
