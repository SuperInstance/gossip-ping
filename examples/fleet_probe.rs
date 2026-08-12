//! Example: Simulated fleet probe cycle.
//!
//! Demonstrates how to use gossip-ping to monitor a fleet of agents.
//! Each probe cycle picks a target, sends a direct ping, and falls back
//! to indirect pings through relays if the direct ping fails.
//!
//! Run with: `cargo run --example fleet_probe`

use gossip_ping::{PingConfig, PingResult, Pinger};
use std::time::Duration;

fn main() {
    // Simulate a fleet of 6 agents
    let fleet: Vec<String> = (0..6).map(|i| format!("agent-{i}")).collect();

    // Create our pinger as agent-0
    let mut pinger = Pinger::new("agent-0", PingConfig::default());

    println!("=== Fleet Probe Simulation ===");
    println!("Fleet: {} nodes", fleet.len());
    println!("Config: timeout={}ms, relays={}, interval={}ms",
        pinger.config().timeout_initial.as_millis(),
        pinger.config().indirect_relay_count,
        pinger.config().probe_interval.as_millis());
    println!();

    // Simulate 12 probe cycles (2 full rounds)
    for cycle in 0..12 {
        let target_index = cycle % (fleet.len() - 1) + 1; // skip self
        let target = &fleet[target_index];

        // Simulate: agent-3 goes down after cycle 4
        let _is_up = target != "agent-3" || cycle < 5;

        let outcome = pinger.full_probe_cycle(
            &fleet,
            target_index,
            |t| {
                if t == "agent-3" && cycle >= 5 {
                    PingResult::Timeout
                } else {
                    let rtt = Duration::from_millis(10 + (cycle as u64 * 2));
                    PingResult::Alive(rtt)
                }
            },
            |relay, target| {
                // Indirect pings: relays can sometimes reach agent-3
                if target == "agent-3" && relay == "agent-5" && cycle < 8 {
                    PingResult::Alive(Duration::from_millis(40))
                } else if target == "agent-3" {
                    PingResult::Timeout
                } else {
                    PingResult::Alive(Duration::from_millis(30))
                }
            },
        );

        if let Some(outcome) = outcome {
            let status = if outcome.suspect { "SUSPECT" } else { "ALIVE" };
            let direct = match &outcome.direct {
                PingResult::Alive(rtt) => format!("{:.1}ms", rtt.as_secs_f64() * 1000.0),
                PingResult::Timeout => "TIMEOUT".to_string(),
                PingResult::Error(e) => format!("ERROR: {e}"),
            };
            let indirect = outcome.indirect.as_ref().map(|r| {
                match r {
                    PingResult::Alive(rtt) => format!("indirect={:.1}ms", rtt.as_secs_f64() * 1000.0),
                    PingResult::Timeout => "indirect=FAIL".to_string(),
                    PingResult::Error(e) => format!("indirect=ERR:{e}"),
                }
            }).unwrap_or_default();

            println!(
                "cycle {:2}: {:>8} | direct={:>10} {:>20} | {}",
                cycle, outcome.target, direct, indirect, status
            );
        }
    }

    // Print RTT statistics
    println!();
    if let Some(stats) = pinger.rtt_stats() {
        println!("=== RTT Statistics ===");
        println!("  samples: {}", stats.samples);
        println!("  min:     {:.1}ms", stats.min.as_secs_f64() * 1000.0);
        println!("  max:     {:.1}ms", stats.max.as_secs_f64() * 1000.0);
        println!("  median:  {:.1}ms", stats.median.as_secs_f64() * 1000.0);
        println!("  avg:     {:.1}ms", stats.avg.as_secs_f64() * 1000.0);
    }

    println!();
    println!("=== Adaptive Timeout ===");
    println!("  current: {:.1}ms", pinger.current_timeout().as_secs_f64() * 1000.0);
    println!("  initial: {:.1}ms", pinger.config().timeout_initial.as_secs_f64() * 1000.0);
}
