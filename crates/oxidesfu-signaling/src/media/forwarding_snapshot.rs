//! Bounded, machine-readable forwarding snapshots produced by the signaling reader heartbeat.
//!
//! The packet path only updates reader-local integer counters. Snapshot construction, JSON
//! serialization, and bounded retention happen on the reader's existing timer window.

use std::{
    collections::VecDeque,
    sync::{Mutex, OnceLock},
};

use serde::Serialize;

const DEFAULT_SNAPSHOT_CAPACITY: usize = 128;
const MAX_TARGETS_PER_SNAPSHOT: usize = 256;

/// A retained forwarding-reader heartbeat for profiler or diagnostic collection.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ForwardingSnapshot {
    pub(crate) schema_version: u8,
    pub(crate) sequence: u64,
    pub(crate) window_duration_ms: u64,
    pub(crate) room: String,
    pub(crate) publisher_identity: String,
    pub(crate) track_sid: String,
    pub(crate) targets: Vec<ForwardingTargetSnapshot>,
}

impl ForwardingSnapshot {
    pub(crate) fn bounded(mut self) -> Self {
        self.targets.truncate(MAX_TARGETS_PER_SNAPSHOT);
        self
    }
}

/// One subscriber target represented in a forwarding snapshot.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct ForwardingTargetSnapshot {
    pub(crate) subscriber_identity: String,
    pub(crate) spatial: SpatialSelectionSnapshot,
    pub(crate) temporal: TemporalSelectionSnapshot,
    pub(crate) rtp_window: RtpWindowSnapshot,
    pub(crate) selector_pli: SelectorPliSnapshot,
    pub(crate) downstream_feedback: DownstreamFeedbackSnapshot,
    pub(crate) forwarding: ForwardingResultSnapshot,
}

/// Target-local spatial selector state at the end of the timer window.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct SpatialSelectionSnapshot {
    pub(crate) source_kind: &'static str,
    pub(crate) maximum: &'static str,
    pub(crate) desired: &'static str,
    pub(crate) current: Option<&'static str>,
    pub(crate) selected_ssrc: Option<u32>,
    pub(crate) selected_rid: Option<String>,
    pub(crate) acquisition_state: &'static str,
    pub(crate) waiting_for: &'static str,
    pub(crate) acquisition_ticks: u8,
    pub(crate) remaining_pli_requests: u8,
    pub(crate) transitions: u64,
}

/// Target-local temporal selector state at the end of the timer window.
#[derive(Debug, Clone, Serialize)]
pub(crate) struct TemporalSelectionSnapshot {
    pub(crate) maximum: Option<&'static str>,
    pub(crate) desired: Option<&'static str>,
    pub(crate) current: Option<&'static str>,
}

/// Successful outgoing RTP writes measured over the heartbeat window.
#[derive(Debug, Clone, Copy, Serialize)]
pub(crate) struct RtpWindowSnapshot {
    pub(crate) packets: u64,
    pub(crate) wire_bytes: u64,
    pub(crate) packets_per_second: u64,
    pub(crate) wire_bytes_per_second: u64,
}

/// Selector-owned PLI attempts and observable non-send reasons for the window.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct SelectorPliSnapshot {
    pub(crate) sent: u64,
    pub(crate) suppressed_stable: u64,
    pub(crate) suppressed_fallback_locked: u64,
    pub(crate) suppressed_budget_exhausted: u64,
    pub(crate) suppressed_retry_or_no_target: u64,
}

/// Downstream PLI/FIR feedback received, forwarded upstream, or suppressed by the RTCP gate.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct DownstreamFeedbackSnapshot {
    pub(crate) pli_received: u64,
    pub(crate) pli_sent: u64,
    pub(crate) pli_suppressed: u64,
    pub(crate) fir_received: u64,
    pub(crate) fir_sent: u64,
    pub(crate) fir_suppressed: u64,
}

/// Rewrite, write, and packet-selection outcomes accumulated for a target.
#[derive(Debug, Clone, Copy, Default, Serialize)]
pub(crate) struct ForwardingResultSnapshot {
    pub(crate) rewrite_drops: u64,
    pub(crate) write_errors: u64,
    pub(crate) drop_waiting_for_keyframe: u64,
    pub(crate) drop_non_selected_ssrc: u64,
    pub(crate) drop_above_maximum: u64,
    pub(crate) drop_unknown_layer: u64,
    pub(crate) drop_temporal_above_maximum: u64,
    pub(crate) drop_temporal_above_desired: u64,
    pub(crate) drop_temporal_timestamp_cap: u64,
}

/// A bounded in-process JSON-lines output path for later profiler collection.
#[derive(Debug)]
pub(crate) struct ForwardingSnapshotStore {
    capacity: usize,
    snapshots: Mutex<VecDeque<ForwardingSnapshot>>,
}

impl ForwardingSnapshotStore {
    pub(crate) fn new(capacity: usize) -> Self {
        Self {
            capacity: capacity.max(1),
            snapshots: Mutex::new(VecDeque::with_capacity(capacity.max(1))),
        }
    }

    /// Retains the newest snapshot, discarding the oldest entry when full.
    pub(crate) fn push(&self, snapshot: ForwardingSnapshot) {
        let Ok(mut snapshots) = self.snapshots.lock() else {
            return;
        };
        if snapshots.len() == self.capacity {
            let _ = snapshots.pop_front();
        }
        snapshots.push_back(snapshot.bounded());
    }

    /// Returns the retained snapshots as one JSON object per line, ordered oldest to newest.
    pub(crate) fn json_lines(&self) -> String {
        let Ok(snapshots) = self.snapshots.lock() else {
            return String::new();
        };
        snapshots
            .iter()
            .filter_map(|snapshot| serde_json::to_string(snapshot).ok())
            .collect::<Vec<_>>()
            .join("\n")
    }
}

fn snapshot_store() -> &'static ForwardingSnapshotStore {
    static STORE: OnceLock<ForwardingSnapshotStore> = OnceLock::new();
    STORE.get_or_init(|| ForwardingSnapshotStore::new(DEFAULT_SNAPSHOT_CAPACITY))
}

/// Records one heartbeat snapshot in the bounded profiler-facing store.
pub(crate) fn record_snapshot(snapshot: ForwardingSnapshot) {
    snapshot_store().push(snapshot);
}

/// Returns the retained forwarding heartbeats as bounded JSON-lines output.
#[allow(dead_code)] // Called by the session's coordinator-facing output path.
pub(crate) fn forwarding_snapshot_json_lines() -> String {
    snapshot_store().json_lines()
}
