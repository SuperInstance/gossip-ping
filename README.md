# Gossip Ping

**A Rust library for the ping/ack health-check mechanism in a gossip-based distributed system**, implementing direct and indirect probes to detect node failures.

## Why It Matters

In SWIM-style gossip protocols, failure detection uses ping/ack probes: node A pings node B, and if B doesn't acknowledge within a timeout, A marks B as suspect. To distinguish network partitions from actual crashes, the protocol supports **indirect pings** — A asks nodes C and D to ping B on A's behalf. If any indirect probe succeeds, B is still alive. This crate implements the probe mechanism, forming the detection layer of the gossip stack.

## How It Works

The ping module sends a direct UDP probe to a target node and waits for an ack response within a configurable timeout. If the direct probe fails, it selects `k` random peers and dispatches indirect ping requests. Each indirect ping has its own (typically longer) timeout. Results are aggregated: any ack (direct or indirect) marks the target as healthy; all failures trigger suspicion. This two-tier approach handles asymmetric network failures where A cannot reach B but C can.

## Quick Start

```rust
// API surface under development — the crate currently provides
// foundational types for ping/ack tracking.
use gossip_ping::add;

fn main() {
    assert_eq!(add(2, 2), 4);
}
```

## API

| Function | Description |
|---|---|
| `add(left, right)` | Placeholder — full ping/ack API under development |

## Architecture Notes

Part of the SuperInstance gossip stack: `gossip-protocol`, `gossip-member`, `gossip-ping`, `gossip-seed`, `gossip-suspicion`. See the [Architecture Guide](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md).

## License

MIT
