//! Prometheus metrics definitions for the sidecar.

use prometheus_client::metrics::counter::Counter;
use prometheus_client::metrics::gauge::Gauge;
use prometheus_client::metrics::histogram::Histogram;
use prometheus_client::registry::Registry;

/// All operational metrics exposed by the sidecar.
#[derive(Debug)]
pub struct SidecarMetrics {
    /// Total number of cross-chain transactions submitted.
    pub xt_received_total: Counter<u64>,
    /// XTs that reached a commit decision.
    pub xt_decided_commit_total: Counter<u64>,
    /// XTs that reached an abort decision.
    pub xt_decided_abort_total: Counter<u64>,
    /// Wall-clock time of each simulation call, in seconds.
    pub simulation_duration_seconds: Histogram,
    /// End-to-end XT latency from submission to decision, in seconds.
    pub xt_decision_latency_seconds: Histogram,
    /// Current number of undecided (in-flight) XTs.
    pub xt_pending_count: Gauge,
    /// Builder polls answered with a hold (undecided XT blocking).
    pub builder_poll_hold_total: Counter<u64>,
    /// Builder polls answered with committed transactions.
    pub builder_poll_deliver_total: Counter<u64>,
    /// Builder polls answered with no transactions and no hold.
    pub builder_poll_empty_total: Counter<u64>,
    /// Inbound CIRC mailbox messages received.
    pub circ_messages_received_total: Counter<u64>,
    /// Outbound CIRC mailbox messages sent.
    pub circ_messages_sent_total: Counter<u64>,
    /// Vote send failures (publisher unreachable or peer send error).
    pub vote_send_failed_total: Counter<u64>,
}

impl SidecarMetrics {
    /// Create and register all metrics into `registry`.
    pub fn new(registry: &mut Registry) -> Self {
        let xt_received_total = Counter::default();
        registry.register(
            "sidecar_xt_received",
            "Total number of cross-chain transactions submitted",
            xt_received_total.clone(),
        );

        let xt_decided_commit_total = Counter::default();
        registry.register(
            "sidecar_xt_decided_commit",
            "Cross-chain transactions that reached a commit decision",
            xt_decided_commit_total.clone(),
        );

        let xt_decided_abort_total = Counter::default();
        registry.register(
            "sidecar_xt_decided_abort",
            "Cross-chain transactions that reached an abort decision",
            xt_decided_abort_total.clone(),
        );

        let simulation_duration_seconds =
            Histogram::new([0.001, 0.005, 0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0].into_iter());
        registry.register(
            "sidecar_simulation_duration_seconds",
            "Wall-clock time of each EVM simulation call",
            simulation_duration_seconds.clone(),
        );

        let xt_decision_latency_seconds =
            Histogram::new([0.01, 0.05, 0.1, 0.25, 0.5, 1.0, 2.5, 5.0, 10.0].into_iter());
        registry.register(
            "sidecar_xt_decision_latency_seconds",
            "End-to-end XT latency from submission to decision",
            xt_decision_latency_seconds.clone(),
        );

        let xt_pending_count = Gauge::default();
        registry.register(
            "sidecar_xt_pending",
            "Current number of undecided in-flight cross-chain transactions",
            xt_pending_count.clone(),
        );

        let builder_poll_hold_total = Counter::default();
        registry.register(
            "sidecar_builder_poll_hold",
            "Builder polls answered with a hold signal",
            builder_poll_hold_total.clone(),
        );

        let builder_poll_deliver_total = Counter::default();
        registry.register(
            "sidecar_builder_poll_deliver",
            "Builder polls that returned committed transactions",
            builder_poll_deliver_total.clone(),
        );

        let builder_poll_empty_total = Counter::default();
        registry.register(
            "sidecar_builder_poll_empty",
            "Builder polls that returned no transactions",
            builder_poll_empty_total.clone(),
        );

        let circ_messages_received_total = Counter::default();
        registry.register(
            "sidecar_circ_messages_received",
            "Inbound CIRC mailbox messages received",
            circ_messages_received_total.clone(),
        );

        let circ_messages_sent_total = Counter::default();
        registry.register(
            "sidecar_circ_messages_sent",
            "Outbound CIRC mailbox messages dispatched",
            circ_messages_sent_total.clone(),
        );

        let vote_send_failed_total = Counter::default();
        registry.register(
            "sidecar_vote_send_failed",
            "Vote send failures (publisher unreachable or peer send error)",
            vote_send_failed_total.clone(),
        );

        Self {
            xt_received_total,
            xt_decided_commit_total,
            xt_decided_abort_total,
            simulation_duration_seconds,
            xt_decision_latency_seconds,
            xt_pending_count,
            builder_poll_hold_total,
            builder_poll_deliver_total,
            builder_poll_empty_total,
            circ_messages_received_total,
            circ_messages_sent_total,
            vote_send_failed_total,
        }
    }
}
