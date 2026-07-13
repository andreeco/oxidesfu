/// Snapshot of signalling relay counters.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RelayMetricsSnapshot {
    pub dispatch_attempts_total: u64,
    pub dispatch_failures_total: u64,
    pub responses_accepted_total: u64,
    pub responses_rejected_total: u64,
    pub fallback_to_local_total: u64,
    pub signal_requests_total: u64,
    pub signal_failures_total: u64,
    pub signal_responses_total: u64,
}

static RELAY_DISPATCH_ATTEMPTS_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RELAY_DISPATCH_FAILURES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RELAY_RESPONSES_ACCEPTED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RELAY_RESPONSES_REJECTED_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RELAY_FALLBACK_TO_LOCAL_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RELAY_SIGNAL_REQUESTS_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RELAY_SIGNAL_FAILURES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);
static RELAY_SIGNAL_RESPONSES_TOTAL: std::sync::atomic::AtomicU64 =
    std::sync::atomic::AtomicU64::new(0);

/// Returns the current signalling relay counter snapshot.
pub fn relay_metrics_snapshot() -> RelayMetricsSnapshot {
    RelayMetricsSnapshot {
        dispatch_attempts_total: RELAY_DISPATCH_ATTEMPTS_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        dispatch_failures_total: RELAY_DISPATCH_FAILURES_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_accepted_total: RELAY_RESPONSES_ACCEPTED_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        responses_rejected_total: RELAY_RESPONSES_REJECTED_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        fallback_to_local_total: RELAY_FALLBACK_TO_LOCAL_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        signal_requests_total: RELAY_SIGNAL_REQUESTS_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        signal_failures_total: RELAY_SIGNAL_FAILURES_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
        signal_responses_total: RELAY_SIGNAL_RESPONSES_TOTAL
            .load(std::sync::atomic::Ordering::Relaxed),
    }
}

pub(crate) fn inc_dispatch_attempts() {
    RELAY_DISPATCH_ATTEMPTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn inc_dispatch_failures() {
    RELAY_DISPATCH_FAILURES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn inc_responses_accepted() {
    RELAY_RESPONSES_ACCEPTED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn inc_responses_rejected() {
    RELAY_RESPONSES_REJECTED_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn inc_fallback_to_local() {
    RELAY_FALLBACK_TO_LOCAL_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn inc_signal_requests() {
    RELAY_SIGNAL_REQUESTS_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn inc_signal_failures() {
    RELAY_SIGNAL_FAILURES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}

pub(crate) fn inc_signal_responses() {
    RELAY_SIGNAL_RESPONSES_TOTAL.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
}
