use axum::{
    http::{HeaderMap, StatusCode, header},
    response::Response,
};
#[cfg(test)]
use axum::{
    extract::{Request, State},
    middleware::Next,
};
use oxidesfu_auth::{AuthContext, AuthError};
use oxidesfu_room::RoomStoreError;

use crate::{state::ApiState, twirp::twirp_error};

pub(crate) fn authenticate(
    state: &ApiState,
    headers: &HeaderMap,
) -> Result<AuthContext, AuthError> {
    let header = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
        .ok_or(AuthError::MissingBearer)?;
    state.auth.verify_authorization_header(header)
}

#[cfg(test)]
pub(crate) async fn optional_auth_middleware(
    State(state): State<ApiState>,
    mut request: Request,
    next: Next,
) -> Response {
    let authorization = match request.headers().get(header::AUTHORIZATION) {
        Some(value) => value,
        None => return next.run(request).await,
    };

    let authorization = match authorization.to_str() {
        Ok(authorization) => authorization,
        Err(_) => return auth_error(AuthError::InvalidToken),
    };

    match state.auth.verify_authorization_header(authorization) {
        Ok(auth) => {
            request.extensions_mut().insert(auth);
            next.run(request).await
        }
        Err(err) => auth_error(err),
    }
}

pub(crate) fn auth_error(err: AuthError) -> Response {
    match err {
        AuthError::MissingBearer | AuthError::InvalidToken | AuthError::InvalidApiKey => {
            twirp_error(
                StatusCode::UNAUTHORIZED,
                "unauthenticated",
                &err.to_string(),
            )
        }
        AuthError::PermissionDenied => {
            twirp_error(StatusCode::FORBIDDEN, "permission_denied", &err.to_string())
        }
    }
}

pub(crate) fn room_store_error(err: RoomStoreError) -> Response {
    match err {
        RoomStoreError::RoomNotFound => {
            twirp_error(StatusCode::NOT_FOUND, "not_found", "room not found")
        }
        RoomStoreError::ParticipantNotFound => {
            twirp_error(StatusCode::NOT_FOUND, "not_found", "participant not found")
        }
        RoomStoreError::AgentDispatchNotFound => twirp_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "agent dispatch not found",
        ),
        RoomStoreError::SipTrunkNotFound => {
            twirp_error(StatusCode::NOT_FOUND, "not_found", "sip trunk not found")
        }
        RoomStoreError::SipDispatchRuleNotFound => twirp_error(
            StatusCode::NOT_FOUND,
            "not_found",
            "sip dispatch rule not found",
        ),
        RoomStoreError::IngressNotFound => {
            twirp_error(StatusCode::NOT_FOUND, "not_found", "ingress not found")
        }
        RoomStoreError::EgressNotFound => {
            twirp_error(StatusCode::NOT_FOUND, "not_found", "egress not found")
        }
        RoomStoreError::MaxParticipantsExceeded => twirp_error(
            StatusCode::TOO_MANY_REQUESTS,
            "resource_exhausted",
            "room has exceeded its max participants",
        ),
        RoomStoreError::InvalidArgument(message) => {
            twirp_error(StatusCode::BAD_REQUEST, "invalid_argument", &message)
        }
        RoomStoreError::LockPoisoned => twirp_error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "internal",
            "internal room store error",
        ),
    }
}
