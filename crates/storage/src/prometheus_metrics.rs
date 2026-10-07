// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.
//
// SPDX-License-Identifier: Apache-2.0
//! # Raft Prometheus metrics (ADR 0031, "Raft / distributed storage" section)
//!
//! Unlike the other ADR 0031 subsystems (audit, auth-plugin, ...), which are
//! pure process-wide counters and are naturally modeled as a single global
//! `static`, Raft state is inherently **per-node-instance**: each Raft node
//! (this process) has its own term/leadership/log-position view. This module
//! therefore exposes [`KeystoneRaftPrometheusMetrics`] as a small struct
//! owned by the storage layer (one instance per `FjallStateMachine`/
//! `app::Storage` pair), not a `static`.
//!
//! Named distinctly from `openraft::RaftMetrics<C>` (re-exported in this
//! crate as `types::RaftMetrics`) to avoid confusion between openraft's own
//! live-metrics snapshot type and this Prometheus-exposition wrapper around
//! it.
//!
//! Five of the six series (`is_leader`, `term`, `last_log_index`,
//! `last_applied_index`, `replication_lag`) are point-in-time gauges derived
//! by reading through to `openraft`'s own metrics watch channel
//! (`Raft::metrics().borrow_watched()`) every time metrics are collected,
//! as OpenTelemetry observable gauges ([`register_gauges`]) — that read is a
//! cheap, non-blocking watch-channel borrow. The sixth,
//! `apply_duration_seconds`, is a real per-operation latency histogram
//! recorded incrementally at the actual state-machine apply call site
//! (`store::state_machine`), since a snapshot read can't reconstruct latency
//! after the fact.

use openstack_keystone_telemetry::metrics::{
    HistogramVec, LATENCY_BUCKETS, Label, Meter, observe_gauge,
};

use crate::TypeConfig;

/// Per-node Raft Prometheus metrics (ADR 0031). See the module docs for why
/// this is a struct instance rather than a `static`, and for the naming
/// rationale relative to `openraft::RaftMetrics`.
pub struct KeystoneRaftPrometheusMetrics {
    /// Recorded incrementally at the actual apply call site
    /// (`store::state_machine::FjallStateMachine::apply`) — a real
    /// per-operation latency measurement, not a snapshot read-through like
    /// the gauges.
    apply_duration_seconds: HistogramVec<0>,
}

impl KeystoneRaftPrometheusMetrics {
    /// Create the apply-latency histogram on `meter`.
    pub fn new(meter: &Meter) -> Self {
        Self {
            apply_duration_seconds: HistogramVec::new(
                meter,
                "keystone_raft_apply_duration_seconds",
                "State-machine apply latency (per committed log entry).",
                [],
                &LATENCY_BUCKETS,
            ),
        }
    }

    /// Record the latency of applying one committed log entry.
    pub fn record_apply(&self, seconds: f64) {
        self.apply_duration_seconds.record(seconds, []);
    }
}

/// Expose the five `keystone_raft_*` gauges on `meter`. `live` returns the
/// current `openraft::RaftMetrics` snapshot and is called on every
/// collection; `node_id` is this node's own Raft id, used to derive
/// `keystone_raft_is_leader`.
///
/// `peer_id` is openraft's own `u64` node id, stringified — a small,
/// config-fixed cluster member set (ADR 0031 cardinality guardrail).
/// `replication_lag` is only reported while this node is the leader: openraft
/// only tracks per-peer replication progress there.
pub fn register_gauges(
    meter: &Meter,
    node_id: u64,
    live: impl Fn() -> openraft::RaftMetrics<TypeConfig> + Clone + Send + Sync + 'static,
) {
    // `Term` for this crate's `TypeConfig` is `u64` (see
    // `proto_impl::impl_leader_id`).
    let gauge = |name: &'static str,
                 help: &'static str,
                 read: fn(&openraft::RaftMetrics<TypeConfig>, u64) -> u64| {
        let live = live.clone();
        observe_gauge(meter, name, help, [], move |emit| {
            emit([], read(&live(), node_id));
        });
    };
    gauge(
        "keystone_raft_is_leader",
        "Whether this node is the current Raft leader (1) or not (0).",
        |m, node_id| u64::from(m.current_leader == Some(node_id)),
    );
    gauge(
        "keystone_raft_term",
        "Current Raft term of this node.",
        |m, _| m.current_term,
    );
    gauge(
        "keystone_raft_last_log_index",
        "Last Raft log index appended to this node's log (tail position).",
        |m, _| m.last_log_index.unwrap_or(0),
    );
    gauge(
        "keystone_raft_last_applied_index",
        "Last Raft log index applied to this node's state machine.",
        |m, _| m.last_applied.as_ref().map(|l| l.index()).unwrap_or(0),
    );
    observe_gauge(
        meter,
        "keystone_raft_replication_lag",
        "Log entries by which a peer's match index trails this leader's \
         last log index (last_log_index - peer match_index); only \
         populated while this node is leader.",
        ["peer_id"],
        move |emit| {
            let metrics = live();
            let last_log_index = metrics.last_log_index.unwrap_or(0);
            for (peer_id, match_log_id) in metrics.replication.iter().flatten() {
                let match_index = match_log_id.as_ref().map(|l| l.index()).unwrap_or(0);
                let peer = peer_id.to_string();
                emit(
                    [Label::bounded(&peer)],
                    last_log_index.saturating_sub(match_index),
                );
            }
        },
    );
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use openraft::RaftMetrics;

    use openstack_keystone_telemetry::metrics::MetricsPipeline;

    use super::*;
    use crate::types::LogId;

    /// Builds a hand-crafted `openraft::RaftMetrics<TypeConfig>` snapshot —
    /// spinning up a real raft cluster is out of scope for a unit test, so
    /// this mirrors the `openraft::RaftMetrics::new_initial` starting point
    /// and overrides the fields this module actually reads.
    fn metrics_fixture(
        node_id: u64,
        current_leader: Option<u64>,
        term: u64,
        last_log_index: Option<u64>,
        last_applied_index: Option<u64>,
        replication: Option<BTreeMap<u64, Option<u64>>>,
    ) -> RaftMetrics<TypeConfig> {
        let mut metrics = RaftMetrics::<TypeConfig>::new_initial(node_id);
        metrics.current_leader = current_leader;
        metrics.current_term = term;
        metrics.last_log_index = last_log_index;
        metrics.last_applied = last_applied_index.map(|idx| LogId::new(term, idx));
        metrics.replication = replication.map(|m| {
            m.into_iter()
                .map(|(id, idx)| (id, idx.map(|i| LogId::new(term, i))))
                .collect()
        });
        metrics
    }

    /// Render `live` the way `/metrics` would, with an apply latency of each
    /// of `applies` seconds recorded.
    fn render(live: RaftMetrics<TypeConfig>, node_id: u64, applies: &[f64]) -> String {
        let pipeline = MetricsPipeline::new();
        let m = KeystoneRaftPrometheusMetrics::new(&pipeline.meter());
        for apply in applies {
            m.record_apply(*apply);
        }
        register_gauges(&pipeline.meter(), node_id, move || live.clone());
        pipeline.render()
    }

    /// Pins the rendered exposition text against `tests/golden/raft.prom`
    /// (ADR 0040).
    #[test]
    fn golden_exposition() {
        let replication = BTreeMap::from([(2u64, Some(90u64)), (3u64, None)]);
        let live = metrics_fixture(1, Some(1), 7, Some(100), Some(95), Some(replication));
        openstack_keystone_metrics::assert_golden!("raft", render(live, 1, &[0.002, 0.2]));
    }

    #[test]
    fn sets_leader_and_position_gauges_when_leader() {
        let text = render(
            metrics_fixture(1, Some(1), 7, Some(100), Some(90), None),
            1,
            &[],
        );
        assert!(text.contains("keystone_raft_is_leader 1\n"));
        assert!(text.contains("keystone_raft_term 7\n"));
        assert!(text.contains("keystone_raft_last_log_index 100\n"));
        assert!(text.contains("keystone_raft_last_applied_index 90\n"));
    }

    #[test]
    fn reports_not_leader_for_a_different_current_leader() {
        let text = render(
            metrics_fixture(2, Some(1), 7, Some(100), Some(90), None),
            2,
            &[],
        );
        assert!(text.contains("keystone_raft_is_leader 0\n"));
    }

    #[test]
    fn reports_not_leader_when_no_leader_elected() {
        let text = render(metrics_fixture(1, None, 3, None, None, None), 1, &[]);
        assert!(text.contains("keystone_raft_is_leader 0\n"));
        assert!(text.contains("keystone_raft_last_log_index 0\n"));
        assert!(text.contains("keystone_raft_last_applied_index 0\n"));
    }

    #[test]
    fn computes_replication_lag_per_peer() {
        let replication = BTreeMap::from([(2u64, Some(80u64)), (3u64, Some(100u64)), (4u64, None)]);
        let text = render(
            metrics_fixture(1, Some(1), 7, Some(100), Some(100), Some(replication)),
            1,
            &[],
        );
        assert!(text.contains("keystone_raft_replication_lag{peer_id=\"2\"} 20\n"));
        assert!(text.contains("keystone_raft_replication_lag{peer_id=\"3\"} 0\n"));
        assert!(text.contains("keystone_raft_replication_lag{peer_id=\"4\"} 100\n"));
        // Unknown/unseen peer has no series.
        assert!(!text.contains("peer_id=\"5\""));
    }

    #[test]
    fn replication_lag_absent_when_not_leader() {
        let text = render(
            metrics_fixture(2, Some(1), 7, Some(100), Some(90), None),
            2,
            &[],
        );
        assert!(!text.contains("keystone_raft_replication_lag{"));
    }

    #[test]
    fn has_all_six_series() {
        let replication = BTreeMap::from([(2u64, Some(90u64))]);
        let live = metrics_fixture(1, Some(1), 7, Some(100), Some(95), Some(replication));
        let text = render(live, 1, &[0.01]);

        assert!(text.contains("# TYPE keystone_raft_is_leader gauge"));
        assert!(text.contains("keystone_raft_is_leader 1\n"));
        assert!(text.contains("# TYPE keystone_raft_term gauge"));
        assert!(text.contains("keystone_raft_term 7\n"));
        assert!(text.contains("# TYPE keystone_raft_last_log_index gauge"));
        assert!(text.contains("keystone_raft_last_log_index 100\n"));
        assert!(text.contains("# TYPE keystone_raft_last_applied_index gauge"));
        assert!(text.contains("keystone_raft_last_applied_index 95\n"));
        assert!(text.contains("# TYPE keystone_raft_replication_lag gauge"));
        assert!(text.contains("keystone_raft_replication_lag{peer_id=\"2\"} 10\n"));
        assert!(text.contains("# TYPE keystone_raft_apply_duration_seconds histogram"));
        assert!(text.contains("keystone_raft_apply_duration_seconds_count 1\n"));
    }

    #[test]
    fn apply_duration_seconds_is_recorded_incrementally_and_rendered() {
        let live = metrics_fixture(1, None, 0, None, None, None);
        let text = render(live, 1, &[0.01, 0.2]);
        assert!(text.contains("keystone_raft_apply_duration_seconds_count 2\n"));
    }
}
