use std::{
    sync::atomic::{AtomicU64, Ordering},
    time::Instant,
};

use axum::{
    body::Body,
    http::{HeaderMap, HeaderValue, Request},
    middleware::Next,
    response::Response,
};

/// Default tracing filter used by the OxideSFU server binary.
pub const DEFAULT_TRACING_ENV_FILTER: &str = "oxidesfu_server=info,tower_http=info";

pub(crate) const X_REQUEST_ID: &str = "x-request-id";

static NEXT_REQUEST_ID: AtomicU64 = AtomicU64::new(1);
static HTTP_REQUESTS_TOTAL: AtomicU64 = AtomicU64::new(0);

pub(crate) fn http_requests_total() -> u64 {
    HTTP_REQUESTS_TOTAL.load(Ordering::Relaxed)
}

pub(crate) async fn attach_request_id(mut request: Request<Body>, next: Next) -> Response {
    HTTP_REQUESTS_TOTAL.fetch_add(1, Ordering::Relaxed);
    let request_id = request_id_from_headers(request.headers());
    let method = request.method().to_string();
    let path = request.uri().path().to_string();
    let start = Instant::now();
    request.extensions_mut().insert(request_id.clone());

    let mut response = next.run(request).await;
    if let Ok(header_value) = HeaderValue::from_str(&request_id) {
        response.headers_mut().insert(X_REQUEST_ID, header_value);
    }

    log_request_completion(
        &request_id,
        &method,
        &path,
        response.status().as_u16(),
        start.elapsed().as_millis(),
    );
    response
}

pub(crate) fn request_id_from_headers(headers: &HeaderMap) -> String {
    if let Some(candidate) = headers
        .get(X_REQUEST_ID)
        .and_then(|value| value.to_str().ok())
        .map(str::trim)
        .filter(|value| !value.is_empty())
    {
        return candidate.to_string();
    }

    format!(
        "req-{:016x}",
        NEXT_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
    )
}

pub(crate) fn log_request_completion(
    request_id: &str,
    method: &str,
    path: &str,
    status: u16,
    latency_ms: u128,
) {
    tracing::info!(
        request_id,
        method,
        path,
        status,
        latency_ms,
        "http_request_completed"
    );
}
