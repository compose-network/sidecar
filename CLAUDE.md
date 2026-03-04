# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

This is **Compose Sidecar** — a cross-chain coordination layer for rollups. It manages cross-chain transaction (XT)
lifecycles: from submission through simulation, peer voting, and committed delivery to builders. Sidecars communicate
with each other over HTTP and with a shared publisher (SP) over QUIC.

## Commands

All common tasks are wrapped in `just` (uses `justfile`):

```sh
just build          # cargo build --workspace
just test           # cargo test --workspace
just lint           # cargo clippy --workspace --all-targets -- -D warnings
just fmt            # cargo fmt --all
just fmt-check      # check formatting without applying
just ci             # fmt-check + lint + test (full local CI)
just ci-full        # ci + cargo-deny + cargo-machete
just run [ARGS]     # cargo run -p sidecar -- [ARGS]
just release        # cargo build --release -p sidecar
just doc            # cargo doc --workspace --no-deps --open
just proto          # regenerate protobuf code (cargo build -p compose-proto)
```

Run a single test:

```sh
cargo test -p <crate-name> <test_name>
# e.g.: cargo test -p compose-coordinator submission
```

Install optional dev tools before running `ci-full`:

```sh
just install-tools  # installs cargo-deny and cargo-machete
just install-hooks  # installs pre-commit hooks (requires: pip install pre-commit)
```

## Toolchain & Formatting

- Rust **1.91** (pinned in `rust-toolchain.toml`)
- `rustfmt.toml`: `max_width = 100`, `imports_granularity = "Crate"`, `group_imports = "StdExternalCrate"`
- Workspace lints in `Cargo.toml` enforce `rust_2018_idioms`, `unused_must_use` (deny), and a broad set of clippy style
  lints
- `openssl` is banned; use `rustls`/`ring` instead

## Architecture

The workspace contains one binary and many library crates, all prefixed `compose-*`:

```
bin/sidecar              — binary entrypoint: wires up all crates and starts HTTP + QUIC
crates/
  primitives             — shared data types (ChainId, XtRequest, PeriodId, etc.)
  primitives-traits      — integration boundary traits (Simulator, MailboxSender, PutInboxBuilder, PublisherClient, CoordinatorError)
  config                 — clap CLI args + env-var config (SIDECAR_* prefix)
  proto                  — protobuf wire types + conversions (prost, rollup_v2)
  coordinator/
    coordinator          — core XT state machine: submission → simulation → voting → decision → delivery
    server               — axum HTTP API (see routes below)
  net/
    transport            — QUIC client/server, TLS (quinn + rustls + rcgen), framing
    publisher            — wraps QuicClient for SP communication
    peer                 — HTTP client for sidecar-to-sidecar coordination
  mailbox               — on-chain mailbox ABI encoding, dependency matching, putInbox queue
  simulation             — RPC-backed tx simulation (eth_call with state overrides)
  metrics                — Prometheus counters/histograms via prometheus-client
  tracing                — tracing-subscriber init (JSON or pretty output)
  adapters/put_inbox     — alloy-based putInbox transaction builder
```

### XT Lifecycle (coordinator pipeline)

1. **Submit** (`POST /xt`): XT arrives, fingerprinted and stored as `PendingXt`
2. **Simulate** (`pipeline/simulation.rs`): eth_call against RPC with per-chain state overlays so XTs see each other's
   effects
3. **Vote** (`handlers/peer_vote.rs`): peer sidecars exchange votes over HTTP (`POST /xt/vote`, `POST /xt/forward`)
4. **Decide** (`decision/`): standalone (single sidecar) or consensus-based commit/abort
5. **Deliver** (`POST /transactions`): builder polls for committed XTs

### HTTP API Routes

| Route                   | Purpose                              |
|-------------------------|--------------------------------------|
| `POST /xt`              | Submit a cross-chain transaction     |
| `GET /xt/:id`           | Get XT status                        |
| `POST /xt/forward`      | Peer forwarding                      |
| `POST /xt/vote`         | Peer vote exchange                   |
| `POST /mailbox`         | Peer mailbox messages                |
| `POST /transactions`    | Builder poll (returns committed XTs) |
| `GET /health`, `/ready` | Liveness/readiness                   |
| `GET /metrics`          | Prometheus metrics                   |

### Publisher (QUIC / SP)

The sidecar optionally connects to a Shared Publisher (SP) via QUIC (`compose-transport`). Messages are length-prefixed
protobuf frames (`compose-proto`). The SP pushes start-period and start-instance messages that drive the coordinator
state machine.

### Configuration

Config is via CLI flags or `SIDECAR_*` env vars (no config file loading at runtime). See `configs/config.example.yaml`
for the full set of options. Key sections: `server`, `publisher`, `chains`, `peers`, `log`.
