use crate::{cleanup::room_cleanup_removed_total, logging::http_requests_total};

pub(crate) async fn metrics() -> String {
    let http_requests = http_requests_total();
    let cleanup_removed = room_cleanup_removed_total();
    let relay = oxidesfu_signaling::relay_metrics_snapshot();

    format!(
        "# HELP oxidesfu_up OxideSFU process health\n# TYPE oxidesfu_up gauge\noxidesfu_up 1\n# HELP oxidesfu_http_requests_total Total HTTP requests served\n# TYPE oxidesfu_http_requests_total counter\noxidesfu_http_requests_total {http_requests}\n# HELP oxidesfu_room_cleanup_removed_total Total rooms removed by periodic cleanup\n# TYPE oxidesfu_room_cleanup_removed_total counter\noxidesfu_room_cleanup_removed_total {cleanup_removed}\n# HELP oxidesfu_relay_dispatch_attempts_total Total non-local relay dispatch attempts\n# TYPE oxidesfu_relay_dispatch_attempts_total counter\noxidesfu_relay_dispatch_attempts_total {}\n# HELP oxidesfu_relay_dispatch_failures_total Total non-local relay dispatch failures\n# TYPE oxidesfu_relay_dispatch_failures_total counter\noxidesfu_relay_dispatch_failures_total {}\n# HELP oxidesfu_relay_responses_accepted_total Total accepted relay responses\n# TYPE oxidesfu_relay_responses_accepted_total counter\noxidesfu_relay_responses_accepted_total {}\n# HELP oxidesfu_relay_responses_rejected_total Total rejected relay responses\n# TYPE oxidesfu_relay_responses_rejected_total counter\noxidesfu_relay_responses_rejected_total {}\n# HELP oxidesfu_relay_fallback_to_local_total Total non-local placement fallbacks to local join\n# TYPE oxidesfu_relay_fallback_to_local_total counter\noxidesfu_relay_fallback_to_local_total {}\n# HELP oxidesfu_relay_signal_requests_total Total relayed long-lived signal requests\n# TYPE oxidesfu_relay_signal_requests_total counter\noxidesfu_relay_signal_requests_total {}\n# HELP oxidesfu_relay_signal_failures_total Total relayed long-lived signal request failures\n# TYPE oxidesfu_relay_signal_failures_total counter\noxidesfu_relay_signal_failures_total {}\n# HELP oxidesfu_relay_signal_responses_total Total relayed long-lived signal responses\n# TYPE oxidesfu_relay_signal_responses_total counter\noxidesfu_relay_signal_responses_total {}\n",
        relay.dispatch_attempts_total,
        relay.dispatch_failures_total,
        relay.responses_accepted_total,
        relay.responses_rejected_total,
        relay.fallback_to_local_total,
        relay.signal_requests_total,
        relay.signal_failures_total,
        relay.signal_responses_total,
    )
}
