# Gossip Ping — Agents Whispering Across Networks

> *Gossip IS stigmergy at network speed. The probe cycle is a pheromone sweep. The suspicion mechanism is an evaporating trail.*

A **failure detection component** implementing the ping/ack mechanism for SWIM-style gossip protocols — sending direct heartbeats to peers, measuring round-trip latency, and triggering suspicion when nodes become unresponsive. This is the liveness probe layer of the fleet's mesh communication stack.

Where [CNS Bridge](https://github.com/SuperInstance/cns-bridge) carries messages through filesystem inboxes and [stigmergy](https://github.com/SuperInstance/stigmergy) leaves pheromone trails in shared environments, Gossip Ping is the **nervous system's proprioception** — the constant, low-level sense of *who is still here*. Every agent pings every other agent. The silence of a missed response is information. The suspicion that follows is a signal to the fleet.

## Why It Matters

In decentralized systems without a central heartbeat collector, failure detection must be distributed. The SWIM protocol's ping mechanism is the first link in the detection chain: node A pings node B directly; if B doesn't respond within a timeout, A marks B as suspect and gossips this suspicion to the fleet. The sensitivity of this probe directly determines detection latency — too short and you get false positives from network jitter; too long and dead nodes waste cluster resources. This library implements the direct-ping phase with configurable timeouts, indirect-ping fallback (asking a third node to relay the ping), and exponential backoff for repeated failures.

## How It Works

**Direct ping**: Node A sends a UDP packet containing `{sender: A, target: B, seq: N}` to node B. B responds with `{from: B, seq: N, status: ALIVE}`. The round-trip time is measured.

**Timeout calculation**: The ping timeout T is adapted based on network conditions:
```
T_initial = 500ms
T_adaptive = median(RTT_history) × 2 + safety_margin
```
If the adaptive timeout exceeds a maximum bound (typically 5s), it's clamped. This prevents pathological network conditions from causing indefinite waits.

**Indirect ping (ping-req)**: If a direct ping times out, node A asks `k` random peers to ping B on A's behalf. This handles the case where A→B connectivity is broken but B is still reachable from other nodes (asymmetric network partition). If any indirect ping succeeds, B is alive. If all fail, B is suspected.

**Probe cycle**: In each gossip round, node A picks one member (round-robin or random) and pings it. This spreads the probing load evenly — O(1) pings per round per node, O(N) total probes per round across the cluster.

**Complexity**:
- Direct ping: O(1) time (one UDP send + one receive)
- Indirect ping with k relays: O(k) messages, O(T_timeout) wall time
- Full probe cycle (all nodes): O(N) rounds, O(1) per round per node

**Comparison with alternatives**:
- Phi-Accrual (Akka): Adaptive threshold based on historical heartbeat arrival distribution → lower false-positive rate but requires more state per node
- Heartbeat-based (Cassandra): Centralized gossip of heartbeat counters → simpler but higher false positives during network partitions
- SWIM ping (this library): Simple, stateless, with indirect fallback → balanced trade-off

## Quick Start

```rust
use gossip_ping::{Pinger, PingConfig};

let config = PingConfig::new()
    .timeout_ms(500)
    .indirect_relay_count(3)
    .probe_interval_ms(1000);

let mut pinger = Pinger::new("node-A", config);
let result = pinger.ping("node-B").await;

match result {
    Ok(rtt) => println!("node-B alive, RTT: {}ms", rtt.as_millis()),
    Err(_) => {
        // Try indirect ping
        let indirect = pinger.indirect_ping("node-B", &["node-C", "node-D"]).await;
        if indirect.is_ok() {
            println!("node-B alive via indirect probe");
        } else {
            println!("node-B suspected dead");
        }
    }
}
```

## API

| Type | Description |
|------|-------------|
| `Pinger::new(self_id, config)` | Create a pinger for node `self_id` |
| `PingConfig` | Configuration (timeout, relay count, probe interval) |
| `.ping(target)` | Send direct UDP ping, return RTT or timeout |
| `.indirect_ping(target, relays)` | Ask relay nodes to ping target on our behalf |
| `.probe_cycle(members)` | Execute one round: pick target, ping, handle result |

## Architecture Notes

Gossip Ping is the liveness probe layer in the SuperInstance gossip stack. It feeds results into gossip-suspicion (state transitions) and gossip-member (membership updates). The configurable timeout maps to **γ** (coordination overhead) in **γ + η = C** — tighter timeouts increase false positives (higher γ from unnecessary suspicion); looser timeouts increase detection latency (higher γ from stale membership). See [Architecture](https://github.com/SuperInstance/SuperInstance/blob/main/ARCHITECTURE.md).

## Fleet Topology

Gossip Ping connects to:

- **[CNS Bridge](https://github.com/SuperInstance/cns-bridge)** — The bus carries messages; gossip ping carries presence. Together: message + liveness.
- **[stigmergy](https://github.com/SuperInstance/stigmergy)** — Gossip IS stigmergy at network speed. The probe cycle is a pheromone sweep.
- **[fleet-envelope](https://github.com/SuperInstance/fleet-envelope)** — Ping results wrapped as fleet events for consumption by other systems.
- **[emergence-engine](https://github.com/SuperInstance/emergence-engine)** — Node liveness data feeds emergence detection (a node going dark is a phase transition).
- **[the-living-minds](https://github.com/SuperInstance/the-living-minds)** — The daemon's warmup pings are a simplified version of this gossip protocol.
- **[confidence-cascade](https://github.com/SuperInstance/confidence-cascade)** — Liveness confidence cascades through the fleet.

---

## Where to Next

- → **[CNS Bridge](https://github.com/SuperInstance/cns-bridge)** — The bus that carries the messages gossip verifies
- → **[stigmergy](https://github.com/SuperInstance/stigmergy)** — Pheromone trails at rest; gossip is pheromone trails in motion
- → **[fleet-envelope](https://github.com/SuperInstance/fleet-envelope)** — The grammar that wraps every ping result
- → **[emergence-engine](https://github.com/SuperInstance/emergence-engine)** — What happens when a node goes dark

---

## References

- Das, A. Gupta, I. & Motivala, A. "SWIM: Scalable Weakly-consistent Infection-style Process Group Membership Protocol," DSN (2002).
- Hayashibara, N. et al. "φ-Accrual Failure Detector," DSN (2004).
-.hashicorp/memberlist: https://github.com/hashicorp/memberlist

## License

MIT
