//! Edge-case tests for gossip-ping: boundary conditions, empty inputs,
//! and extreme values.
//!
//! The fleet standard: no NaN should propagate silently. No empty input
//! should crash. Every boundary is a test.

use gossip_ping::{
    AckMessage, PingConfig, PingResult, Pinger, ProbeOutcome,
};
use std::time::{Duration, Instant};

// ──────────────────────────────────────────────
// Empty Input Edge Cases
// ──────────────────────────────────────────────

#[test]
fn probe_cycle_empty_members_returns_none() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let outcome = pinger.probe_cycle(&[], 0, |_| PingResult::Timeout);
    assert!(outcome.is_none());
}

#[test]
fn full_probe_cycle_empty_members_returns_none() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let outcome = pinger.full_probe_cycle(
        &[],
        0,
        |_| PingResult::Timeout,
        |_, _| PingResult::Timeout,
    );
    assert!(outcome.is_none());
}

#[test]
fn full_probe_cycle_single_member_excludes_self() {
    // If the only member is the pinger itself, relays should be empty
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members = vec!["A".to_string()];

    let outcome = pinger.full_probe_cycle(
        &members,
        0,
        |_target| PingResult::Timeout,
        |_, _| PingResult::Alive(Duration::from_millis(10)),
    );

    // Should probe self? members[0] = "A" which is the pinger.
    // But probe_cycle doesn't skip self — that's the caller's job.
    let outcome = outcome.unwrap();
    assert!(outcome.suspect);
    // No relays available (only member is self)
    assert!(outcome.indirect.is_none());
}

#[test]
fn indirect_ping_empty_relays_returns_error() {
    let pinger = Pinger::new("A", PingConfig::default());
    let result = pinger.indirect_ping(&"B".to_string(), &[], |_, _| PingResult::Timeout);
    match result {
        PingResult::Error(msg) => assert!(msg.contains("no relays")),
        _ => panic!("Expected Error for empty relays"),
    }
}

#[test]
fn indirect_ping_fewer_relays_than_config() {
    // Config says 5 relays, but only 2 available — should use 2
    let pinger = Pinger::new("A", PingConfig::default().indirect_relay_count(5));
    let relays: Vec<String> = (0..2).map(|i| format!("relay-{i}")).collect();

    let result = pinger.indirect_ping(&"target".to_string(), &relays, |relay, target| {
        PingResult::Alive(Duration::from_millis(15))
    });

    assert!(result.is_alive());
}

// ──────────────────────────────────────────────
// Index Wrapping
// ──────────────────────────────────────────────

#[test]
fn probe_cycle_index_wraps_around() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members: Vec<String> = (0..3).map(|i| format!("node-{i}")).collect();

    // Index 5 with 3 members → member[5 % 3] = member[2]
    let outcome = pinger
        .probe_cycle(&members, 5, |_target| {
            PingResult::Alive(Duration::from_millis(10))
        })
        .unwrap();
    assert_eq!(outcome.target, "node-2");
}

#[test]
fn probe_cycle_index_zero() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members = vec!["X".into(), "Y".into()];

    let outcome = pinger
        .probe_cycle(&members, 0, |_| PingResult::Alive(Duration::from_millis(5)))
        .unwrap();
    assert_eq!(outcome.target, "X");
}

#[test]
fn full_probe_cycle_index_wraps_correctly() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members = vec!["A".into(), "B".into(), "C".into(), "D".into()];

    // Index 10 with 4 members → member[10 % 4] = member[2] = "C"
    let outcome = pinger
        .full_probe_cycle(
            &members,
            10,
            |t| {
                if t == "C" {
                    PingResult::Alive(Duration::from_millis(10))
                } else {
                    PingResult::Timeout
                }
            },
            |_, _| PingResult::Timeout,
        )
        .unwrap();

    assert_eq!(outcome.target, "C");
    assert!(!outcome.suspect);
}

// ──────────────────────────────────────────────
// RTT History Edge Cases
// ──────────────────────────────────────────────

#[test]
fn rtt_stats_single_sample() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    pinger.record_rtt(Duration::from_millis(100));

    let stats = pinger.rtt_stats().unwrap();
    assert_eq!(stats.samples, 1);
    assert_eq!(stats.min, Duration::from_millis(100));
    assert_eq!(stats.max, Duration::from_millis(100));
    assert_eq!(stats.median, Duration::from_millis(100));
}

#[test]
fn rtt_stats_two_samples() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    pinger.record_rtt(Duration::from_millis(50));
    pinger.record_rtt(Duration::from_millis(150));

    let stats = pinger.rtt_stats().unwrap();
    assert_eq!(stats.samples, 2);
    // Median of even-length array: sorted[mid] = sorted[1] = 150
    // (integer division: len/2 = 1, so sorted[1])
    assert_eq!(stats.min, Duration::from_millis(50));
    assert_eq!(stats.max, Duration::from_millis(150));
}

#[test]
fn rtt_history_evicts_oldest_when_full() {
    let config = PingConfig {
        rtt_history_size: 3,
        ..PingConfig::default()
    };
    let mut pinger = Pinger::new("A", config);

    pinger.record_rtt(Duration::from_millis(100));
    pinger.record_rtt(Duration::from_millis(200));
    pinger.record_rtt(Duration::from_millis(300));
    pinger.record_rtt(Duration::from_millis(400)); // evicts 100

    let stats = pinger.rtt_stats().unwrap();
    assert_eq!(stats.samples, 3);
    assert_eq!(stats.min, Duration::from_millis(200));
    assert_eq!(stats.max, Duration::from_millis(400));
}

#[test]
fn reset_clears_rtt_history() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    pinger.record_rtt(Duration::from_millis(100));
    assert!(pinger.rtt_stats().is_some());

    pinger.reset_rtt_history();
    assert!(pinger.rtt_stats().is_none());
}

#[test]
fn current_timeout_after_reset_is_initial() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    pinger.record_rtt(Duration::from_millis(10));
    pinger.record_rtt(Duration::from_millis(20));
    // Now timeout is adaptive, not initial
    pinger.reset_rtt_history();
    // After reset, timeout should be initial
    assert_eq!(
        pinger.current_timeout(),
        PingConfig::default().timeout_initial
    );
}

// ──────────────────────────────────────────────
// Timeout Calculation Boundaries
// ──────────────────────────────────────────────

#[test]
fn current_timeout_with_zero_rtt() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    pinger.record_rtt(Duration::from_millis(0));

    // median(0) * 2 + safety = 0 + 50ms = 50ms
    let timeout = pinger.current_timeout();
    assert_eq!(timeout, Duration::from_millis(50));
}

#[test]
fn current_timeout_clamped_to_max() {
    let config = PingConfig {
        timeout_max: Duration::from_millis(100),
        timeout_initial: Duration::from_millis(500),
        ..PingConfig::default()
    };
    let mut pinger = Pinger::new("A", config);

    // Record a very high RTT
    pinger.record_rtt(Duration::from_secs(10));

    // median(10s) * 2 + 50ms = 20.05s, but clamped to 100ms
    let timeout = pinger.current_timeout();
    assert_eq!(timeout, Duration::from_millis(100));
}

#[test]
fn current_timeout_with_max_rtt_values() {
    let mut pinger = Pinger::new("A", PingConfig::default());

    // Fill history with identical large values
    for _ in 0..16 {
        pinger.record_rtt(Duration::from_secs(1));
    }

    // median(1s) * 2 + 50ms = 2.05s, within 5s max
    let timeout = pinger.current_timeout();
    assert_eq!(timeout, Duration::from_millis(2050));
}

// ──────────────────────────────────────────────
// Probe Cycle with All-Timeout
// ──────────────────────────────────────────────

#[test]
fn full_probe_cycle_all_relays_fail() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members = vec!["A".into(), "B".into(), "C".into(), "D".into()];

    let outcome = pinger
        .full_probe_cycle(
            &members,
            1, // target = "B"
            |_| PingResult::Timeout,
            |_, _| PingResult::Timeout,
        )
        .unwrap();

    assert!(outcome.suspect);
    assert!(outcome.indirect.is_some());
    assert!(outcome.indirect.unwrap().is_timeout());
}

#[test]
fn full_probe_cycle_all_error() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members = vec!["A".into(), "B".into(), "C".into()];

    let outcome = pinger
        .full_probe_cycle(
            &members,
            1,
            |_| PingResult::Error("network unreachable".into()),
            |_, _| PingResult::Error("relay failed".into()),
        )
        .unwrap();

    assert!(outcome.suspect);
}

#[test]
fn full_probe_cycle_direct_error_indirect_alive() {
    let mut pinger = Pinger::new("A", PingConfig::default());
    let members = vec!["A".into(), "B".into(), "C".into()];

    let outcome = pinger
        .full_probe_cycle(
            &members,
            1, // target = "B"
            |_| PingResult::Error("direct failed".into()),
            |_, target| {
                if target == "B" {
                    PingResult::Alive(Duration::from_millis(30))
                } else {
                    PingResult::Timeout
                }
            },
        )
        .unwrap();

    // Indirect succeeded → not suspect
    assert!(!outcome.suspect);
}

// ──────────────────────────────────────────────
// Sequence Number Boundaries
// ──────────────────────────────────────────────

#[test]
fn seq_num_starts_at_zero() {
    let mut p = Pinger::new("X", PingConfig::default());
    assert_eq!(p.next_seq(), 0);
}

#[test]
fn make_ping_increments_seq() {
    let mut p = Pinger::new("A", PingConfig::default());
    let _ = p.make_ping(&"B".into());
    let _ = p.make_ping(&"C".into());
    let msg3 = p.make_ping(&"D".into());
    assert_eq!(msg3.seq, 2);
}

// ──────────────────────────────────────────────
// Config Extremes
// ──────────────────────────────────────────────

#[test]
fn config_zero_indirect_relays() {
    let config = PingConfig {
        indirect_relay_count: 0,
        ..PingConfig::default()
    };
    let pinger = Pinger::new("A", config);

    let result = pinger.indirect_ping(&"B".to_string(), &["C".into()], |_, _| {
        PingResult::Alive(Duration::from_millis(10))
    });

    // With 0 relay count, should return Error("no relays")
    match result {
        PingResult::Error(_) => {}
        _ => panic!("Expected Error with 0 indirect relays"),
    }
}

#[test]
fn config_zero_rtt_history_size() {
    let config = PingConfig {
        rtt_history_size: 0,
        ..PingConfig::default()
    };
    let mut pinger = Pinger::new("A", config);

    // With rtt_history_size=0, the VecDeque has capacity 0.
    // record_rtt checks len() >= rtt_history_size (0 >= 0 = true),
    // so it pops_front then pushes. The sample IS recorded.
    pinger.record_rtt(Duration::from_millis(50));
    // Document actual behavior: sample is recorded despite size=0
    assert!(pinger.rtt_stats().is_some());
    assert_eq!(pinger.rtt_stats().unwrap().samples, 1);
}

#[test]
fn config_zero_timeout_max() {
    let config = PingConfig {
        timeout_max: Duration::from_millis(0),
        timeout_initial: Duration::from_millis(500),
        ..PingConfig::default()
    };
    let mut pinger = Pinger::new("A", config);
    pinger.record_rtt(Duration::from_millis(100));

    // median(100ms) * 2 + 50ms = 250ms, but clamped to 0
    let timeout = pinger.current_timeout();
    assert_eq!(timeout, Duration::from_millis(0));
}

#[test]
fn config_zero_safety_margin() {
    let config = PingConfig {
        safety_margin: Duration::from_millis(0),
        ..PingConfig::default()
    };
    let mut pinger = Pinger::new("A", config);
    pinger.record_rtt(Duration::from_millis(100));

    // median(100ms) * 2 + 0 = 200ms
    assert_eq!(pinger.current_timeout(), Duration::from_millis(200));
}

// ──────────────────────────────────────────────
// PingResult Variants
// ──────────────────────────────────────────────

#[test]
fn ping_result_error_is_neither_alive_nor_timeout() {
    let err = PingResult::Error("test".into());
    assert!(!err.is_alive());
    assert!(!err.is_timeout());
}

#[test]
fn ping_result_alive_with_zero_duration() {
    let alive = PingResult::Alive(Duration::from_millis(0));
    assert!(alive.is_alive());
    assert!(!alive.is_timeout());
}

// ──────────────────────────────────────────────
// Serde Feature Tests
// ──────────────────────────────────────────────

#[test]
fn serde_roundtrip_ping_result_alive() {
    let original = PingResult::Alive(Duration::from_millis(42));
    let json = serde_json::to_string(&original).unwrap();
    let deserialized: PingResult = serde_json::from_str(&json).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn serde_roundtrip_ping_result_timeout() {
    let original = PingResult::Timeout;
    let json = serde_json::to_string(&original).unwrap();
    let deserialized: PingResult = serde_json::from_str(&json).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn serde_roundtrip_ack_message() {
    let original = AckMessage::alive("node-B", 999);
    let json = serde_json::to_string(&original).unwrap();
    let deserialized: AckMessage = serde_json::from_str(&json).unwrap();
    assert_eq!(original, deserialized);
}

#[test]
fn serde_roundtrip_probe_outcome() {
    let original = ProbeOutcome {
        target: "node-X".into(),
        direct: PingResult::Timeout,
        indirect: Some(PingResult::Alive(Duration::from_millis(15))),
        suspect: false,
    };
    let json = serde_json::to_string(&original).unwrap();
    let deserialized: ProbeOutcome = serde_json::from_str(&json).unwrap();
    assert_eq!(original.target, deserialized.target);
    assert!(deserialized.suspect == false);
}
