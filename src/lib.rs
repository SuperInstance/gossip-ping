//! gossip-ping: SWIM-style failure detection with direct ping, indirect
//! ping-req fallback, and adaptive timeout based on RTT history.
//!
//! See README.md for the full protocol description.

#[cfg(feature = "serde")]
use serde::{Deserialize, Serialize};

use std::collections::VecDeque;
use std::time::{Duration, Instant};

/// A node identifier in the gossip cluster.
pub type NodeId = String;

/// Sequence number for correlating ping/ack pairs.
pub type SeqNum = u64;

/// The result of a ping attempt.
///
/// # Example
///
/// ```
/// use gossip_ping::PingResult;
/// use std::time::Duration;
///
/// let alive = PingResult::Alive(Duration::from_millis(42));
/// assert!(alive.is_alive());
/// assert!(!alive.is_timeout());
/// ```
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub enum PingResult {
    /// The node responded. The duration is the round-trip time.
    Alive(Duration),
    /// The node did not respond within the timeout.
    Timeout,
    /// The ping could not be sent (network error, invalid address, etc.).
    Error(String),
}

impl PingResult {
    pub fn is_alive(&self) -> bool {
        matches!(self, PingResult::Alive(_))
    }

    pub fn is_timeout(&self) -> bool {
        matches!(self, PingResult::Timeout)
    }
}

/// What happened in a full probe cycle (direct + optional indirect).
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct ProbeOutcome {
    pub target: NodeId,
    pub direct: PingResult,
    pub indirect: Option<PingResult>,
    /// Whether the target should be marked suspect after this cycle.
    pub suspect: bool,
}

/// Wire format for a ping message.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PingMessage {
    pub sender: NodeId,
    pub target: NodeId,
    pub seq: SeqNum,
}

/// Wire format for an ack message.
#[derive(Debug, Clone, PartialEq)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct AckMessage {
    pub from: NodeId,
    pub seq: SeqNum,
    pub alive: bool,
}

impl PingMessage {
    pub fn new(sender: impl Into<NodeId>, target: impl Into<NodeId>, seq: SeqNum) -> Self {
        Self {
            sender: sender.into(),
            target: target.into(),
            seq,
        }
    }
}

impl AckMessage {
    pub fn alive(from: impl Into<NodeId>, seq: SeqNum) -> Self {
        Self {
            from: from.into(),
            seq,
            alive: true,
        }
    }

    pub fn dead(from: impl Into<NodeId>, seq: SeqNum) -> Self {
        Self {
            from: from.into(),
            seq,
            alive: false,
        }
    }
}

/// Configuration for the pinger. Defaults are based on the SWIM paper.
///
/// # Example
///
/// ```
/// use gossip_ping::PingConfig;
/// use std::time::Duration;
///
/// let config = PingConfig::new()
///     .timeout_ms(750)
///     .indirect_relay_count(5)
///     .probe_interval_ms(2000);
///
/// assert_eq!(config.timeout_initial, Duration::from_millis(750));
/// ```
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct PingConfig {
    /// Initial ping timeout before any RTT history exists.
    pub timeout_initial: Duration,
    /// Maximum ping timeout (clamp for adaptive calculation).
    pub timeout_max: Duration,
    /// Safety margin added to the adaptive timeout.
    pub safety_margin: Duration,
    /// Number of relay nodes for indirect ping (ping-req).
    pub indirect_relay_count: usize,
    /// Interval between probe cycles.
    pub probe_interval: Duration,
    /// How many RTT samples to keep for adaptive timeout.
    pub rtt_history_size: usize,
}

impl Default for PingConfig {
    fn default() -> Self {
        Self {
            timeout_initial: Duration::from_millis(500),
            timeout_max: Duration::from_secs(5),
            safety_margin: Duration::from_millis(50),
            indirect_relay_count: 3,
            probe_interval: Duration::from_secs(1),
            rtt_history_size: 16,
        }
    }
}

impl PingConfig {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn timeout_ms(self, ms: u64) -> Self {
        Self {
            timeout_initial: Duration::from_millis(ms),
            ..self
        }
    }

    pub fn indirect_relay_count(self, count: usize) -> Self {
        Self {
            indirect_relay_count: count,
            ..self
        }
    }

    pub fn probe_interval_ms(self, ms: u64) -> Self {
        Self {
            probe_interval: Duration::from_millis(ms),
            ..self
        }
    }
}

/// The pinger state: identity, config, sequence counter, and RTT history.
///
/// # Example
///
/// ```
/// use gossip_ping::{Pinger, PingConfig};
///
/// let mut pinger = Pinger::new("node-A", PingConfig::default());
/// let seq0 = pinger.next_seq();
/// let seq1 = pinger.next_seq();
/// assert_eq!(seq0, 0);
/// assert_eq!(seq1, 1);
/// ```
#[derive(Debug)]
pub struct Pinger {
    self_id: NodeId,
    config: PingConfig,
    next_seq: SeqNum,
    rtt_history: VecDeque<Duration>,
}

impl Pinger {
    pub fn new(self_id: impl Into<NodeId>, config: PingConfig) -> Self {
        let rtt_capacity = config.rtt_history_size;
        Self {
            self_id: self_id.into(),
            config,
            next_seq: 0,
            rtt_history: VecDeque::with_capacity(rtt_capacity),
        }
    }

    pub fn self_id(&self) -> &str {
        &self.self_id
    }

    pub fn config(&self) -> &PingConfig {
        &self.config
    }

    /// Allocate the next sequence number.
    pub fn next_seq(&mut self) -> SeqNum {
        let seq = self.next_seq;
        self.next_seq += 1;
        seq
    }

    /// Create a ping message for `target`.
    pub fn make_ping(&mut self, target: &NodeId) -> PingMessage {
        let seq = self.next_seq();
        PingMessage::new(&self.self_id, target, seq)
    }

    /// The current adaptive timeout, based on RTT history.
    /// With no history, uses the initial timeout.
    pub fn current_timeout(&self) -> Duration {
        if self.rtt_history.is_empty() {
            return self.config.timeout_initial;
        }

        // median(RTT_history) × 2 + safety_margin
        let mut sorted: Vec<Duration> = self.rtt_history.iter().copied().collect();
        sorted.sort();
        let mid = sorted.len() / 2;
        let median = sorted[mid];
        let adaptive = median * 2 + self.config.safety_margin;

        adaptive.min(self.config.timeout_max)
    }

    /// Record a successful RTT measurement.
    pub fn record_rtt(&mut self, rtt: Duration) {
        if self.rtt_history.len() >= self.config.rtt_history_size {
            self.rtt_history.pop_front();
        }
        self.rtt_history.push_back(rtt);
    }

    /// Process an ack: record RTT if alive, return the PingResult.
    pub fn handle_ack(&mut self, ack: &AckMessage, sent_at: Instant) -> PingResult {
        if ack.alive {
            let rtt = sent_at.elapsed();
            self.record_rtt(rtt);
            PingResult::Alive(rtt)
        } else {
            PingResult::Timeout
        }
    }

    /// Execute one probe cycle against `members`. Picks one target (round-robin
    /// via `index`), performs a direct ping, and if that fails, tries indirect
    /// ping through up to `indirect_relay_count` relays.
    ///
    /// This is the pure-logic version: the caller provides a `ping_fn` that
    /// performs the actual network send/receive and returns a PingResult.
    pub fn probe_cycle<F>(
        &mut self,
        members: &[NodeId],
        index: usize,
        mut ping_fn: F,
    ) -> Option<ProbeOutcome>
    where
        F: FnMut(&NodeId) -> PingResult,
    {
        if members.is_empty() {
            return None;
        }

        let target = &members[index % members.len()];

        // Direct ping
        let direct = ping_fn(target);

        match direct {
            PingResult::Alive(_) => Some(ProbeOutcome {
                target: target.clone(),
                direct,
                indirect: None,
                suspect: false,
            }),
            _ => {
                // Direct failed — the caller should do indirect pings externally.
                // We record the failure and mark suspect.
                Some(ProbeOutcome {
                    target: target.clone(),
                    direct,
                    indirect: None,
                    suspect: true,
                })
            }
        }
    }

    /// Execute an indirect ping through relay nodes.
    ///
    /// The caller provides `relay_fn` that pings `target` via `relay`.
    /// Returns Alive if any relay succeeds, otherwise the last error.
    pub fn indirect_ping<F>(
        &self,
        target: &NodeId,
        relays: &[NodeId],
        mut relay_fn: F,
    ) -> PingResult
    where
        F: FnMut(&NodeId, &NodeId) -> PingResult,
    {
        let count = relays.len().min(self.config.indirect_relay_count);
        if count == 0 {
            return PingResult::Error("no relays available".into());
        }

        let mut last = PingResult::Timeout;
        for relay in &relays[..count] {
            last = relay_fn(relay, target);
            if last.is_alive() {
                return last;
            }
        }
        last
    }

    /// Execute a full SWIM probe cycle: direct ping, and if that fails,
    /// indirect ping through relay nodes before marking suspect.
    ///
    /// This is the recommended entry point for SWIM-style failure detection.
    /// `ping_fn` performs the direct ping; `relay_fn` performs an indirect
    /// ping via a relay node. Relays are selected from `members` excluding
    /// the caller and the target.
    ///
    /// # Example
    ///
    /// ```
    /// use gossip_ping::{Pinger, PingConfig, PingResult};
    ///
    /// let mut pinger = Pinger::new("A", PingConfig::default());
    /// let members = vec!["A".into(), "B".into(), "C".into(), "D".into()];
    ///
    /// let outcome = pinger.full_probe_cycle(
    ///     &members,
    ///     1, // index into members (probe "B")
    ///     |target| PingResult::Alive(std::time::Duration::from_millis(10)),
    ///     |relay, target| PingResult::Alive(std::time::Duration::from_millis(20)),
    /// ).unwrap();
    ///
    /// assert!(!outcome.suspect);
    /// ```
    ///
    /// Returns `None` if members is empty. Otherwise returns a `ProbeOutcome`
    /// with `indirect` set if an indirect ping was attempted.
    pub fn full_probe_cycle<F, G>(
        &mut self,
        members: &[NodeId],
        index: usize,
        mut ping_fn: F,
        mut relay_fn: G,
    ) -> Option<ProbeOutcome>
    where
        F: FnMut(&NodeId) -> PingResult,
        G: FnMut(&NodeId, &NodeId) -> PingResult,
    {
        if members.is_empty() {
            return None;
        }

        let target = &members[index % members.len()];

        // Direct ping
        let direct = ping_fn(target);

        if direct.is_alive() {
            return Some(ProbeOutcome {
                target: target.clone(),
                direct,
                indirect: None,
                suspect: false,
            });
        }

        // Direct failed — try indirect through available relays
        let relays: Vec<NodeId> = members
            .iter()
            .filter(|m| *m != target && *m != &self.self_id)
            .cloned()
            .collect();

        let indirect = if relays.is_empty() {
            None
        } else {
            Some(self.indirect_ping(target, &relays, &mut relay_fn))
        };

        let suspect = !indirect.as_ref().map_or(false, |r| r.is_alive());

        Some(ProbeOutcome {
            target: target.clone(),
            direct,
            indirect,
            suspect,
        })
    }

    /// Clear RTT history (e.g., after a network change).
    pub fn reset_rtt_history(&mut self) {
        self.rtt_history.clear();
    }

    /// Current RTT statistics, if any history exists.
    pub fn rtt_stats(&self) -> Option<RttStats> {
        if self.rtt_history.is_empty() {
            return None;
        }

        let mut sorted: Vec<Duration> = self.rtt_history.iter().copied().collect();
        sorted.sort();

        let min = sorted[0];
        let max = sorted[sorted.len() - 1];
        let mid = sorted.len() / 2;
        let median = sorted[mid];
        let sum: Duration = sorted.iter().sum();
        let avg = sum / sorted.len() as u32;

        Some(RttStats {
            min,
            max,
            median,
            avg,
            samples: sorted.len(),
        })
    }
}

/// Aggregated RTT statistics.
#[derive(Debug, Clone)]
#[cfg_attr(feature = "serde", derive(Serialize, Deserialize))]
pub struct RttStats {
    pub min: Duration,
    pub max: Duration,
    pub median: Duration,
    pub avg: Duration,
    pub samples: usize,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ping_message_has_sender_target_seq() {
        let msg = PingMessage::new("A", "B", 42);
        assert_eq!(msg.sender, "A");
        assert_eq!(msg.target, "B");
        assert_eq!(msg.seq, 42);
    }

    #[test]
    fn ack_alive_and_dead() {
        let alive = AckMessage::alive("B", 1);
        assert!(alive.alive);
        let dead = AckMessage::dead("B", 1);
        assert!(!dead.alive);
    }

    #[test]
    fn pinger_allocates_increasing_seq() {
        let mut p = Pinger::new("A", PingConfig::default());
        assert_eq!(p.next_seq(), 0);
        assert_eq!(p.next_seq(), 1);
        assert_eq!(p.next_seq(), 2);
    }

    #[test]
    fn make_ping_includes_self_id() {
        let mut p = Pinger::new("node-A", PingConfig::default());
        let msg = p.make_ping(&"node-B".into());
        assert_eq!(msg.sender, "node-A");
        assert_eq!(msg.target, "node-B");
    }

    #[test]
    fn initial_timeout_is_config_default() {
        let p = Pinger::new("A", PingConfig::default());
        assert_eq!(p.current_timeout(), Duration::from_millis(500));
    }

    #[test]
    fn timeout_adapts_to_rtt_history() {
        let mut p = Pinger::new("A", PingConfig::default());
        // Record some RTTs: 100ms, 100ms, 100ms
        for _ in 0..3 {
            p.record_rtt(Duration::from_millis(100));
        }
        // median(100) * 2 + 50ms safety = 250ms
        assert_eq!(p.current_timeout(), Duration::from_millis(250));
    }

    #[test]
    fn timeout_clamped_to_max() {
        let mut p = Pinger::new("A", PingConfig::default());
        // Record very large RTTs
        for _ in 0..10 {
            p.record_rtt(Duration::from_secs(10));
        }
        // median(10s) * 2 + 50ms = 20.05s, clamped to 5s
        assert_eq!(p.current_timeout(), Duration::from_secs(5));
    }

    #[test]
    fn rtt_history_is_bounded() {
        let mut p = Pinger::new("A", PingConfig {
            rtt_history_size: 4,
            ..PingConfig::default()
        });

        for i in 0..10 {
            p.record_rtt(Duration::from_millis(i * 10));
        }

        let stats = p.rtt_stats().unwrap();
        assert_eq!(stats.samples, 4);
        // Last 4 values: 60, 70, 80, 90 ms
        assert_eq!(stats.min, Duration::from_millis(60));
        assert_eq!(stats.max, Duration::from_millis(90));
    }

    #[test]
    fn rtt_stats_reported_correctly() {
        let mut p = Pinger::new("A", PingConfig::default());
        p.record_rtt(Duration::from_millis(50));
        p.record_rtt(Duration::from_millis(100));
        p.record_rtt(Duration::from_millis(150));

        let stats = p.rtt_stats().unwrap();
        assert_eq!(stats.min, Duration::from_millis(50));
        assert_eq!(stats.max, Duration::from_millis(150));
        assert_eq!(stats.median, Duration::from_millis(100));
        assert_eq!(stats.samples, 3);
    }

    #[test]
    fn handle_ack_alive_records_rtt() {
        let mut p = Pinger::new("A", PingConfig::default());
        let sent_at = Instant::now();
        // tiny sleep to ensure elapsed > 0
        std::thread::sleep(Duration::from_millis(1));
        let ack = AckMessage::alive("B", 0);
        let result = p.handle_ack(&ack, sent_at);
        assert!(result.is_alive());
        assert!(p.rtt_stats().is_some());
    }

    #[test]
    fn handle_ack_dead_returns_timeout() {
        let mut p = Pinger::new("A", PingConfig::default());
        let sent_at = Instant::now();
        let ack = AckMessage::dead("B", 0);
        let result = p.handle_ack(&ack, sent_at);
        assert!(result.is_timeout());
        assert!(p.rtt_stats().is_none());
    }

    #[test]
    fn probe_cycle_alive_does_not_mark_suspect() {
        let mut p = Pinger::new("A", PingConfig::default());
        let members = vec!["B".into(), "C".into()];
        let outcome = p.probe_cycle(&members, 0, |_| {
            PingResult::Alive(Duration::from_millis(10))
        });
        let outcome = outcome.unwrap();
        assert!(!outcome.suspect);
        assert!(outcome.direct.is_alive());
        assert!(outcome.indirect.is_none());
    }

    #[test]
    fn probe_cycle_timeout_marks_suspect() {
        let mut p = Pinger::new("A", PingConfig::default());
        let members = vec!["B".into(), "C".into()];
        let outcome = p.probe_cycle(&members, 0, |_| PingResult::Timeout);
        let outcome = outcome.unwrap();
        assert!(outcome.suspect);
        assert!(outcome.direct.is_timeout());
    }

    #[test]
    fn probe_cycle_empty_members_returns_none() {
        let mut p = Pinger::new("A", PingConfig::default());
        let outcome = p.probe_cycle(&[], 0, |_| PingResult::Timeout);
        assert!(outcome.is_none());
    }

    #[test]
    fn probe_cycle_wraps_around_index() {
        let mut p = Pinger::new("A", PingConfig::default());
        let members = vec!["B".into(), "C".into()];
        // index 5 with 2 members → member[1] = "C"
        let outcome = p.probe_cycle(&members, 5, |target| {
            assert_eq!(target, "C");
            PingResult::Alive(Duration::from_millis(5))
        });
        assert!(!outcome.unwrap().suspect);
    }

    #[test]
    fn indirect_ping_succeeds_if_any_relay_reaches_target() {
        let p = Pinger::new("A", PingConfig::default());
        let relays: Vec<NodeId> = vec!["C".into(), "D".into(), "E".into()];
        let mut calls = 0;
        let result = p.indirect_ping(&"B".into(), &relays, |_relay, _target| {
            calls += 1;
            if calls == 2 {
                PingResult::Alive(Duration::from_millis(50))
            } else {
                PingResult::Timeout
            }
        });
        assert!(result.is_alive());
        assert_eq!(calls, 2); // stopped after success
    }

    #[test]
    fn indirect_ping_fails_if_all_relays_fail() {
        let p = Pinger::new("A", PingConfig::default());
        let relays: Vec<NodeId> = vec!["C".into(), "D".into(), "E".into()];
        let result = p.indirect_ping(&"B".into(), &relays, |_, _| PingResult::Timeout);
        assert!(result.is_timeout());
    }

    #[test]
    fn indirect_ping_respects_relay_count_limit() {
        let p = Pinger::new("A", PingConfig {
            indirect_relay_count: 2,
            ..PingConfig::default()
        });
        let relays: Vec<NodeId> = vec!["C".into(), "D".into(), "E".into()];
        let mut calls = 0;
        let _ = p.indirect_ping(&"B".into(), &relays, |_, _| {
            calls += 1;
            PingResult::Timeout
        });
        assert_eq!(calls, 2); // limited by indirect_relay_count
    }

    #[test]
    fn indirect_ping_no_relays_returns_error() {
        let p = Pinger::new("A", PingConfig::default());
        let result = p.indirect_ping(&"B".into(), &[], |_, _| PingResult::Timeout);
        assert!(matches!(result, PingResult::Error(_)));
    }

    #[test]
    fn reset_clears_history() {
        let mut p = Pinger::new("A", PingConfig::default());
        p.record_rtt(Duration::from_millis(50));
        p.record_rtt(Duration::from_millis(100));
        assert!(p.rtt_stats().is_some());

        p.reset_rtt_history();
        assert!(p.rtt_stats().is_none());
        // Back to initial timeout
        assert_eq!(p.current_timeout(), Duration::from_millis(500));
    }

    #[test]
    fn config_builder_methods_work() {
        let config = PingConfig::new()
            .timeout_ms(750)
            .indirect_relay_count(5)
            .probe_interval_ms(2000);

        assert_eq!(config.timeout_initial, Duration::from_millis(750));
        assert_eq!(config.indirect_relay_count, 5);
        assert_eq!(config.probe_interval, Duration::from_millis(2000));
    }

    #[test]
    fn ping_result_predicates() {
        assert!(PingResult::Alive(Duration::from_millis(10)).is_alive());
        assert!(!PingResult::Timeout.is_alive());
        assert!(PingResult::Timeout.is_timeout());
        assert!(!PingResult::Alive(Duration::ZERO).is_timeout());
    }

    // -----------------------------------------------------------------------
    // full_probe_cycle: integrated direct + indirect SWIM probe
    // -----------------------------------------------------------------------

    #[test]
    fn full_probe_cycle_direct_success_no_indirect() {
        let mut p = Pinger::new("A", PingConfig::default());
        let members = vec!["A".into(), "B".into(), "C".into()];
        let outcome = p
            .full_probe_cycle(
                &members,
                1, // probe B
                |_| PingResult::Alive(Duration::from_millis(10)),
                |_, _| panic!("indirect should not be called"),
            )
            .unwrap();
        assert!(!outcome.suspect);
        assert!(outcome.direct.is_alive());
        assert!(outcome.indirect.is_none());
    }

    #[test]
    fn full_probe_cycle_indirect_recovery_not_suspect() {
        let mut p = Pinger::new("A", PingConfig::default());
        let members = vec!["A".into(), "B".into(), "C".into(), "D".into()];
        // Direct ping to B fails, but indirect via C succeeds
        let outcome = p
            .full_probe_cycle(
                &members,
                1, // probe B
                |target| {
                    if target == "B" {
                        PingResult::Timeout
                    } else {
                        PingResult::Alive(Duration::from_millis(10))
                    }
                },
                |relay, target| {
                    assert_eq!(target, "B");
                    if relay == "C" {
                        PingResult::Alive(Duration::from_millis(50))
                    } else {
                        PingResult::Timeout
                    }
                },
            )
            .unwrap();
        assert!(outcome.direct.is_timeout());
        assert!(outcome.indirect.as_ref().unwrap().is_alive());
        assert!(!outcome.suspect); // recovered via indirect
    }

    #[test]
    fn full_probe_cycle_all_fail_marks_suspect() {
        let mut p = Pinger::new("A", PingConfig::default());
        let members = vec!["A".into(), "B".into(), "C".into(), "D".into()];
        let outcome = p
            .full_probe_cycle(
                &members,
                1, // probe B
                |_| PingResult::Timeout,
                |_, _| PingResult::Timeout,
            )
            .unwrap();
        assert!(outcome.direct.is_timeout());
        assert!(outcome.indirect.as_ref().unwrap().is_timeout());
        assert!(outcome.suspect);
    }

    #[test]
    fn full_probe_cycle_excludes_self_and_target_from_relays() {
        let mut p = Pinger::new("A", PingConfig::default());
        let members = vec!["A".into(), "B".into(), "C".into(), "D".into()];
        let mut relays_used = Vec::new();
        let _outcome = p
            .full_probe_cycle(
                &members,
                1, // probe B
                |_| PingResult::Timeout,
                |relay, _target| {
                    relays_used.push(relay.clone());
                    PingResult::Timeout
                },
            )
            .unwrap();
        // Should try C and D, never A (self) or B (target)
        assert!(relays_used.contains(&"C".to_string()));
        assert!(relays_used.contains(&"D".to_string()));
        assert!(!relays_used.contains(&"A".to_string()));
        assert!(!relays_used.contains(&"B".to_string()));
    }

    #[test]
    fn full_probe_cycle_empty_members_returns_none() {
        let mut p = Pinger::new("A", PingConfig::default());
        assert!(p
            .full_probe_cycle(&[], 0, |_| PingResult::Timeout, |_, _| PingResult::Timeout)
            .is_none());
    }

    #[test]
    fn full_probe_cycle_no_relays_available() {
        let mut p = Pinger::new("A", PingConfig::default());
        // Only A and B in members — no possible relays
        let members = vec!["A".into(), "B".into()];
        let outcome = p
            .full_probe_cycle(
                &members,
                1, // probe B
                |_| PingResult::Timeout,
                |_, _| panic!("no relays should be attempted"),
            )
            .unwrap();
        assert!(outcome.direct.is_timeout());
        assert!(outcome.indirect.is_none());
        assert!(outcome.suspect);
    }
}
