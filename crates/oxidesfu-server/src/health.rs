use axum::Json;
use serde::Serialize;

/// Health response returned by OxideSFU's health endpoint.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct HealthResponse {
    /// Stable service identifier for operators and tests.
    pub service: &'static str,
    /// Current health status.
    pub status: &'static str,
}

pub(crate) async fn healthz() -> Json<HealthResponse> {
    Json(HealthResponse {
        service: "ferrite",
        status: "ok",
    })
}
