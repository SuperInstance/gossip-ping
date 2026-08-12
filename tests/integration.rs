//! Integration tests for gossip-ping.
//!
//! These tests exercise the public API as an external consumer would,
//! focusing on multi-step scenarios, edge cases, and realistic probe cycles.

use gossip_ping::{
    AckMessage, PingConfig, PingResult, Pinger, ProbeOutcome,
};
use std::time::{Duration, Instant};

// ---------------------------------------------------------------------------
// Multi-node probe cycle simulation
// ---------------------------------------------------------------------------

#[test]
fn full_probe_cycle_round_robin_across_fleet() {
    let mut pinger = Pinger::new("node-A", PingConfig::default());
    let fleet: Vec<_> = (0..5).map(|i| format!("node-{i}")).collect();

    // Cycle through all 5 nodes twice
    let mut probed = Vec::new();
    for i in 0..10 {
        let outcome = pinger
            .probe_cycle(&fleet, i, |target| {
                probed.push(target.clone());
                PingResult::Alive(Duration::from_millis(20))
            })
            .unwrap();
        assert!(!outcome.suspect);
    }

    // Round-robin: indices 0..10 mod 5 → 0,1,2,3,4,0,1,2,3,4
    let expected: Vec<_> = (0..10).map(|i| format!("node-{}", i % 5)).collect();
    assert_eq!(probed, expected);
}

#[test]
fn probe_cycle_with_intermittent_failures() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members = vec!["B".into(), "C".into(), "D".into()];

    // B responds, C times out, D responds
    let outcomes: Vec<ProbeOutcome> = members
        .iter()
        .enumerate()
        .map(|(i, _)| {
            pinger
                .probe_cycle(&members, i, |target| {
                    if target == "C" {
                        PingResult::Timeout
                    } else {
                        PingResult::Alive(Duration::from_millis(15))
                    }
                })
                .unwrap()
        })
        .collect();

    assert!(!outcomes[0].suspect); // B
    assert!(outcomes[1].suspect); // C
    assert!(!outcomes[2].suspect); // D
}

// ---------------------------------------------------------------------------
// Adaptive timeout behavior over time
// ---------------------------------------------------------------------------

#[test]
fn timeout_decreases_when_network_improves() {
    let mut p = Pinger::new("A", PingConfig::default());

    // Start with bad network: 300ms RTTs
    for _ in 0..16 {
        p.record_rtt(Duration::from_millis(300));
    }
    let slow_timeout = p.current_timeout();

    // Network improves: 20ms RTTs fully replace the bounded history
    for _ in 0..16 {
        p.record_rtt(Duration::from_millis(20));
    }
    let fast_timeout = p.current_timeout();

    assert!(
        fast_timeout < slow_timeout,
        "timeout should decrease as network improves: {} vs {}",
        fast_timeout.as_millis(),
        slow_timeout.as_millis()
    );
}

#[test]
fn timeout_is_stable_under_consistent_latency() {
    let mut p = Pinger::new("A", PingConfig::default());

    // Consistent 100ms RTTs
    for _ in 0..16 {
        p.record_rtt(Duration::from_millis(100));
    }

    // median(100) * 2 + 50ms = 250ms — should be stable
    let t1 = p.current_timeout();
    p.record_rtt(Duration::from_millis(100));
    let t2 = p.current_timeout();
    assert_eq!(t1, t2);
}

#[test]
fn timeout_with_single_sample_uses_median_directly() {
    let mut p = Pinger::new("A", PingConfig::default());
    p.record_rtt(Duration::from_millis(100));

    // median of [100] = 100; 100*2 + 50 = 250
    assert_eq!(p.current_timeout(), Duration::from_millis(250));
}

#[test]
fn timeout_with_even_sample_count_uses_upper_median() {
    let mut p = Pinger::new("A", PingConfig::default());
    // Two samples: 50ms and 150ms
    p.record_rtt(Duration::from_millis(50));
    p.record_rtt(Duration::from_millis(150));

    // sorted = [50, 150], len=2, mid=1, median=150
    // 150*2 + 50 = 350ms
    assert_eq!(p.current_timeout(), Duration::from_millis(350));
}

// ---------------------------------------------------------------------------
// Indirect ping (ping-req) scenarios
// ---------------------------------------------------------------------------

#[test]
fn indirect_ping_with_first_relay_succeeding() {
    let p = Pinger::new("A", PingConfig::default());
    let relays = vec!["C".into(), "D".into(), "E".into()];

    let result = p.indirect_ping(&"B".into(), &relays, |relay, target| {
        assert_eq!(target, "B");
        if relay == "C" {
            PingResult::Alive(Duration::from_millis(30))
        } else {
            panic!("should not try other relays after first success");
        }
    });

    assert!(result.is_alive());
}

#[test]
fn indirect_ping_with_exact_relay_count() {
    let p = Pinger::new("A", PingConfig {
        indirect_relay_count: 3,
        ..PingConfig::default()
    });

    let relays = vec!["X".into(), "Y".into(), "Z".into()];
    let mut attempts = 0;

    let result = p.indirect_ping(&"B".into(), &relays, |_, _| {
        attempts += 1;
        PingResult::Timeout
    });

    assert!(result.is_timeout());
    assert_eq!(attempts, 3);
}

#[test]
fn indirect_ping_fewer_relays_than_config() {
    let p = Pinger::new("A", PingConfig {
        indirect_relay_count: 5,
        ..PingConfig::default()
    });

    // Only 2 relays available, config asks for 5 — should use all 2
    let relays = vec!["C".into(), "D".into()];
    let mut attempts = 0;

    let result = p.indirect_ping(&"B".into(), &relays, |_, _| {
        attempts += 1;
        PingResult::Timeout
    });

    assert!(result.is_timeout());
    assert_eq!(attempts, 2);
}

// ---------------------------------------------------------------------------
// RTT statistics edge cases
// ---------------------------------------------------------------------------

#[test]
fn rtt_stats_single_sample() {
    let mut p = Pinger::new("A", PingConfig::default());
    p.record_rtt(Duration::from_millis(42));

    let stats = p.rtt_stats().unwrap();
    assert_eq!(stats.min, Duration::from_millis(42));
    assert_eq!(stats.max, Duration::from_millis(42));
    assert_eq!(stats.median, Duration::from_millis(42));
    assert_eq!(stats.avg, Duration::from_millis(42));
    assert_eq!(stats.samples, 1);
}

#[test]
fn rtt_stats_avg_is_mean() {
    let mut p = Pinger::new("A", PingConfig::default());
    p.record_rtt(Duration::from_millis(30));
    p.record_rtt(Duration::from_millis(60));
    p.record_rtt(Duration::from_millis(90));

    let stats = p.rtt_stats().unwrap();
    // (30+60+90)/3 = 60
    assert_eq!(stats.avg, Duration::from_millis(60));
}

#[test]
fn rtt_stats_after_reset_are_none() {
    let mut p = Pinger::new("A", PingConfig::default());
    p.record_rtt(Duration::from_millis(50));
    p.reset_rtt_history();
    assert!(p.rtt_stats().is_none());
}

// ---------------------------------------------------------------------------
// Config builder edge cases
// ---------------------------------------------------------------------------

#[test]
fn config_default_matches_swim_paper() {
    let c = PingConfig::default();
    // SWIM paper defaults
    assert_eq!(c.timeout_initial, Duration::from_millis(500));
    assert_eq!(c.timeout_max, Duration::from_secs(5));
    assert_eq!(c.indirect_relay_count, 3);
    assert_eq!(c.probe_interval, Duration::from_secs(1));
}

#[test]
fn config_builder_chains_correctly() {
    let c = PingConfig::new()
        .timeout_ms(1000)
        .indirect_relay_count(7)
        .probe_interval_ms(5000);

    assert_eq!(c.timeout_initial, Duration::from_millis(1000));
    assert_eq!(c.indirect_relay_count, 7);
    assert_eq!(c.probe_interval, Duration::from_secs(5));
    // Non-chained fields retain defaults
    assert_eq!(c.timeout_max, Duration::from_secs(5));
}

// ---------------------------------------------------------------------------
// Message construction
// ---------------------------------------------------------------------------

#[test]
fn ping_message_round_trip() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let ping = pinger.make_ping(&"B".into());

    // Simulate ack from B
    let ack = AckMessage::alive("B", ping.seq);
    assert_eq!(ack.seq, ping.seq);
    assert!(ack.alive);
    assert_eq!(ack.from, "B");
}

#[test]
fn ack_dead_carries_seq_for_correlation() {
    let ack = AckMessage::dead("node-X", 999);
    assert_eq!(ack.seq, 999);
    assert!(!ack.alive);
}

// ---------------------------------------------------------------------------
// Sequence number monotonicity
// ---------------------------------------------------------------------------

#[test]
fn seq_numbers_are_monotonically_increasing() {
    let mut p = Pinger::new("A", PingConfig::default());
    let mut seqs = Vec::new();
    for _ in 0..100 {
        seqs.push(p.next_seq());
    }
    for i in 0..99 {
        assert!(seqs[i] < seqs[i + 1], "seq {} >= seq {}", seqs[i], seqs[i + 1]);
    }
}

#[test]
fn make_ping_advances_seq() {
    let mut p = Pinger::new("A", PingConfig::default());
    let m1 = p.make_ping(&"B".into());
    let m2 = p.make_ping(&"C".into());
    assert!(m2.seq > m1.seq);
}

// ---------------------------------------------------------------------------
// Error variant in PingResult
// ---------------------------------------------------------------------------

#[test]
fn ping_result_error_is_neither_alive_nor_timeout() {
    let err = PingResult::Error("network unreachable".into());
    assert!(!err.is_alive());
    assert!(!err.is_timeout());
}

#[test]
fn probe_cycle_error_marks_suspect() {
    let mut p = Pinger::new("A", PingConfig::default());
    let members = vec!["B".into()];
    let outcome = p
        .probe_cycle(&members, 0, |_| PingResult::Error("no route".into()))
        .unwrap();
    assert!(outcome.suspect);
}

// ---------------------------------------------------------------------------
// Handle ack with real timing
// ---------------------------------------------------------------------------

#[test]
fn handle_ack_measures_actual_elapsed_time() {
    let mut p = Pinger::new("A", PingConfig::default());
    let sent_at = Instant::now();
    std::thread::sleep(Duration::from_millis(5));
    let ack = AckMessage::alive("B", 0);
    let result = p.handle_ack(&ack, sent_at);

    if let PingResult::Alive(rtt) = result {
        assert!(rtt >= Duration::from_millis(5));
    } else {
        panic!("expected Alive");
    }
}

// ---------------------------------------------------------------------------
// Serde serialization (only when feature is enabled)
// ---------------------------------------------------------------------------

#[cfg(feature = "serde")]
mod serde_tests {
    use gossip_ping::{AckMessage, PingMessage, PingResult};
    use serde_json;
    use std::time::Duration;

    #[test]
    fn ping_message_serializes_roundtrip() {
        let msg = PingMessage::new("node-A", "node-B", 42);
        let json = serde_json::to_string(&msg).unwrap();
        let deserialized: PingMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(msg, deserialized);
    }

    #[test]
    fn ack_message_serializes_roundtrip() {
        let ack = AckMessage::alive("node-B", 7);
        let json = serde_json::to_string(&ack).unwrap();
        assert!(json.contains("\"alive\":true"));
        let deserialized: AckMessage = serde_json::from_str(&json).unwrap();
        assert_eq!(ack, deserialized);
    }

    #[test]
    fn ping_result_alive_serializes_with_rtt() {
        let result = PingResult::Alive(Duration::from_millis(42));
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("Alive"));
        let back: PingResult = serde_json::from_str(&json).unwrap();
        assert!(back.is_alive());
    }

    #[test]
    fn ping_result_timeout_serializes() {
        let result = PingResult::Timeout;
        let json = serde_json::to_string(&result).unwrap();
        assert_eq!(json, "\"Timeout\"");
    }

    #[test]
    fn ping_result_error_serializes_with_message() {
        let result = PingResult::Error("network unreachable".into());
        let json = serde_json::to_string(&result).unwrap();
        assert!(json.contains("network unreachable"));
    }
}
