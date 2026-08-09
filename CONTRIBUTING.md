# Contributing to gossip-ping

## What This Is

`gossip-ping` is a Rust library implementing the ping/ack mechanism for SWIM-style gossip protocols. It sends direct heartbeats to peers, measures round-trip latency, and triggers suspicion when nodes become unresponsive. Includes indirect ping fallback (ping-req) for asymmetric network partitions and adaptive timeout based on RTT history.

## Development Setup

```bash
git clone git@github.com:SuperInstance/gossip-ping.git
cd gossip-ping
cargo build
cargo test
```

### Prerequisites

- Rust 1.75+ (edition 2021)
- No system dependencies required

## Running Tests

```bash
# All tests (lib + integration)
cargo test

# Lib tests only
cargo test --lib

# Integration tests only
cargo test --test integration

# Doc tests
cargo test --doc

# Run a specific test
cargo test <test_name>

# With output
cargo test -- --nocapture
```

### Test Organization

| File | What It Tests |
|------|--------------|
| `src/lib.rs` (inline `#[cfg(test)]`) | Unit tests: PingResult, ProbeOutcome, message types, timeout calculation |
| `tests/integration.rs` | Integration tests: full probe cycles, indirect ping flows, RTT history |

## Project Structure

```
gossip-ping/
├── src/
│   └── lib.rs             # Core: NodeId, PingResult, ProbeOutcome, PingMessage, RTT history, probe cycle
├── tests/
│   └── integration.rs     # Full probe cycle integration tests
├── Cargo.toml
└── README.md
```

## Code Style

- **Rust:** idiomatic Rust, edition 2021
- **Doc comments:** every public function and type needs a doc comment with at least one example
- **Types:** strong typing for wire formats (`PingMessage`, `AckMessage`) and results (`PingResult`, `ProbeOutcome`)
- **Tests:** every new probe behavior or timeout calculation needs both unit and integration test coverage
- **Commits:** conventional commits (`feat:`, `fix:`, `test:`, `docs:`, `chore:`, `refactor:`)
- **Minimize dependencies:** network protocol crates should stay lean

## Key Design Decisions

1. **SWIM-style probing.** Direct ping first; if that times out, indirect ping (ping-req) via `k` random relay peers. This handles asymmetric partitions.
2. **Adaptive timeout.** Timeout is based on median RTT history with a safety margin, clamped to a maximum. This adapts to network conditions without unbounded waits.
3. **Probe cycle.** Each node probes one member per round (round-robin or random). This spreads load evenly — O(1) pings per round per node.
4. **RTT history.** Maintained as a `VecDeque<Duration>` with configurable window size. Median is used for timeout calculation (robust against outliers).
5. **Suspicion, not death.** A failed probe cycle marks a node as suspect, not dead. Suspicion is gossiped; confirmation requires independent corroboration.

## Pull Request Checklist

- [ ] `cargo test` passes (lib + integration + doc tests)
- [ ] New code has both unit and integration test coverage
- [ ] Public APIs documented with examples
- [ ] No secrets or credentials committed
- [ ] No unnecessary dependencies added
- [ ] Documentation updated if behavior changed
- [ ] Commit messages follow conventional commits

## Fleet Context

This crate is part of the SuperInstance fleet. It provides the failure detection layer for distributed fleet infrastructure — the first link in the SWIM detection chain.

Related fleet components:
- `stigmergy` — bio-inspired coordination that assumes fleet membership (provided by gossip)
- `eisenstein` — hexagonal lattice math used in fleet spatial reasoning
- Fleet membership management — gossip-ping feeds suspicion events into the membership layer
