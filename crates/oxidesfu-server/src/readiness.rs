use std::sync::Arc;

use axum::{Json, response::IntoResponse};
use oxidesfu_room::RoomNodeDirectory;
use serde::Serialize;

/// Reports readiness of relay backend dependencies for distributed signalling.
pub trait RelayBackendReadiness: Send + Sync {
    /// Returns true when the relay backend is healthy enough for routing non-local relay intents.
    fn is_ready(&self) -> bool;
}

/// Default relay backend readiness probe that always reports ready.
#[derive(Debug)]
pub struct AlwaysReadyRelayBackendReadiness;

impl RelayBackendReadiness for AlwaysReadyRelayBackendReadiness {
    fn is_ready(&self) -> bool {
        true
    }
}

/// Readiness response returned by OxideSFU's readiness endpoint.
#[derive(Debug, Clone, Serialize, PartialEq, Eq)]
pub struct ReadinessResponse {
    /// Stable service identifier for operators and tests.
    pub service: &'static str,
    /// Current readiness status.
    pub status: &'static str,
}

pub(crate) async fn readinessz(
    room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    relay_backend_readiness: Arc<dyn RelayBackendReadiness>,
) -> axum::response::Response {
    if !relay_backend_readiness.is_ready() {
        return (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadinessResponse {
                service: "oxidesfu",
                status: "not_ready",
            }),
        )
            .into_response();
    }
    let Some(room_nodes) = room_nodes else {
        return (
            axum::http::StatusCode::OK,
            Json(ReadinessResponse {
                service: "oxidesfu",
                status: "ready",
            }),
        )
            .into_response();
    };

    match room_nodes.list_nodes() {
        Ok(nodes) => {
            let has_serving_node = nodes.iter().any(|node| {
                room_nodes
                    .is_node_draining(&node.id)
                    .is_ok_and(|draining| !draining)
            });
            if has_serving_node {
                (
                    axum::http::StatusCode::OK,
                    Json(ReadinessResponse {
                        service: "oxidesfu",
                        status: "ready",
                    }),
                )
                    .into_response()
            } else {
                (
                    axum::http::StatusCode::SERVICE_UNAVAILABLE,
                    Json(ReadinessResponse {
                        service: "oxidesfu",
                        status: "not_ready",
                    }),
                )
                    .into_response()
            }
        }
        Err(_) => (
            axum::http::StatusCode::SERVICE_UNAVAILABLE,
            Json(ReadinessResponse {
                service: "oxidesfu",
                status: "not_ready",
            }),
        )
            .into_response(),
    }
}
