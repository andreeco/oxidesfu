use std::collections::HashMap;

use axum::{
    extract::Query,
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use base64::{Engine as _, engine::general_purpose::URL_SAFE};
use oxidesfu_auth::{AuthContext, AuthError};
use oxidesfu_protocol::{JoinRequestDecodeError, decode_join_request_param};

use serde::Deserialize;

use crate::state::SignalState;

#[derive(Debug, Deserialize)]
pub(crate) struct SignalQuery {
    pub(crate) access_token: Option<String>,
    pub(crate) join_request: Option<String>,
    pub(crate) reconnect: Option<String>,
    pub(crate) sid: Option<String>,
    pub(crate) auto_subscribe: Option<String>,
    pub(crate) auto_subscribe_data_track: Option<String>,
    pub(crate) attributes: Option<String>,
    pub(crate) protocol: Option<i32>,
    pub(crate) sdk: Option<String>,
    pub(crate) os: Option<String>,
    pub(crate) device_model: Option<String>,
}

pub(crate) struct ValidatedJoin {
    pub(crate) auth: AuthContext,
    pub(crate) request: livekit_protocol::JoinRequest,
}

pub(crate) fn validate_join(
    state: &SignalState,
    headers: &HeaderMap,
    query: &SignalQuery,
    needs_join_request: bool,
) -> Result<ValidatedJoin, Box<Response>> {
    let auth = authenticate(state, headers, query).map_err(signal_auth_error)?;
    auth.ensure_join_permission().map_err(signal_auth_error)?;

    let Some(join_request) = query.join_request.as_deref() else {
        if needs_join_request {
            return Err(Box::new(
                (StatusCode::BAD_REQUEST, "join_request is required").into_response(),
            ));
        }
        return Ok(ValidatedJoin {
            auth,
            request: legacy_join_request_from_query(query),
        });
    };

    let request =
        decode_join_request_param(join_request).map_err(|err| Box::new(join_decode_error(err)))?;
    Ok(ValidatedJoin { auth, request })
}

fn legacy_join_request_from_query(query: &SignalQuery) -> livekit_protocol::JoinRequest {
    let mut request = livekit_protocol::JoinRequest::default();

    if query_reconnect_requested(query) {
        request.reconnect = true;
        if let Some(participant_sid) = query.sid.as_deref() {
            request.participant_sid = participant_sid.to_string();
        }
    }

    let auto_subscribe = query_bool(query.auto_subscribe.as_deref());
    let auto_subscribe_data_track = query_bool(query.auto_subscribe_data_track.as_deref());
    if auto_subscribe.is_some() || auto_subscribe_data_track.is_some() {
        request.connection_settings = Some(livekit_protocol::ConnectionSettings {
            auto_subscribe: auto_subscribe.unwrap_or(true),
            auto_subscribe_data_track,
            ..Default::default()
        });
    }

    if let Some(attributes) = decode_participant_attributes(query.attributes.as_deref()) {
        request.participant_attributes = attributes;
    }

    let mut client_info = livekit_protocol::ClientInfo::default();
    let mut has_client_info = false;

    if let Some(protocol) = query.protocol {
        client_info.protocol = protocol;
        has_client_info = true;
    }
    if let Some(os) = query.os.as_deref() {
        client_info.os = os.to_string();
        has_client_info = true;
    }
    if let Some(device_model) = query.device_model.as_deref() {
        client_info.device_model = device_model.to_string();
        has_client_info = true;
    }
    if query.sdk.is_some() {
        has_client_info = true;
    }

    if has_client_info {
        request.client_info = Some(client_info);
    }

    request
}

fn decode_participant_attributes(encoded: Option<&str>) -> Option<HashMap<String, String>> {
    let encoded = encoded?;
    let bytes = URL_SAFE.decode(encoded).ok()?;
    serde_json::from_slice(&bytes).ok()
}

pub(crate) fn authenticate(
    state: &SignalState,
    headers: &HeaderMap,
    query: &SignalQuery,
) -> Result<AuthContext, AuthError> {
    if let Some(header) = headers
        .get(header::AUTHORIZATION)
        .and_then(|value| value.to_str().ok())
    {
        return state.auth.verify_authorization_header(header);
    }

    query
        .access_token
        .as_deref()
        .ok_or(AuthError::MissingBearer)
        .and_then(|token| state.auth.verify_token(token))
}

pub(crate) fn signal_auth_error(err: AuthError) -> Response {
    match err {
        AuthError::MissingBearer | AuthError::InvalidToken | AuthError::InvalidApiKey => {
            (StatusCode::UNAUTHORIZED, err.to_string()).into_response()
        }
        AuthError::PermissionDenied => (StatusCode::FORBIDDEN, err.to_string()).into_response(),
    }
}

pub(crate) fn join_decode_error(err: JoinRequestDecodeError) -> Response {
    (StatusCode::BAD_REQUEST, err.to_string()).into_response()
}

fn query_bool(value: Option<&str>) -> Option<bool> {
    match value {
        Some("1") | Some("true") | Some("TRUE") | Some("True") => Some(true),
        Some("0") | Some("false") | Some("FALSE") | Some("False") => Some(false),
        _ => None,
    }
}

fn query_reconnect_requested(query: &SignalQuery) -> bool {
    query_bool(query.reconnect.as_deref()).unwrap_or(false)
}

#[allow(dead_code)]
pub(crate) async fn _parse_query(Query(query): Query<SignalQuery>) -> Query<SignalQuery> {
    Query(query)
}

#[cfg(test)]
mod tests {
    use super::{SignalQuery, legacy_join_request_from_query};
    use base64::{Engine as _, engine::general_purpose::URL_SAFE};
    use std::collections::HashMap;

    #[test]
    fn legacy_query_maps_connection_settings_and_attributes_into_join_request() {
        let attrs = URL_SAFE.encode(br#"{"b":"2","c":"3"}"#);
        let query = SignalQuery {
            access_token: None,
            join_request: None,
            reconnect: None,
            sid: None,
            auto_subscribe: Some("false".to_string()),
            auto_subscribe_data_track: Some("false".to_string()),
            attributes: Some(attrs),
            protocol: Some(17),
            sdk: Some("go".to_string()),
            os: Some("android".to_string()),
            device_model: Some("Xiaomi 2201117TI".to_string()),
        };

        let request = legacy_join_request_from_query(&query);
        let connection_settings = request
            .connection_settings
            .expect("connection settings should be present");
        assert!(!connection_settings.auto_subscribe);
        assert_eq!(connection_settings.auto_subscribe_data_track, Some(false));
        assert_eq!(
            request.participant_attributes,
            HashMap::from([
                ("b".to_string(), "2".to_string()),
                ("c".to_string(), "3".to_string())
            ])
        );

        let client_info = request.client_info.expect("client info should be present");
        assert_eq!(client_info.protocol, 17);
        assert_eq!(client_info.os, "android");
        assert_eq!(client_info.device_model, "Xiaomi 2201117TI");
    }

    #[test]
    fn legacy_query_reconnect_maps_sid() {
        let query = SignalQuery {
            access_token: None,
            join_request: None,
            reconnect: Some("true".to_string()),
            sid: Some("PA_sid123".to_string()),
            auto_subscribe: None,
            auto_subscribe_data_track: None,
            attributes: None,
            protocol: None,
            sdk: None,
            os: None,
            device_model: None,
        };

        let request = legacy_join_request_from_query(&query);
        assert!(request.reconnect);
        assert_eq!(request.participant_sid, "PA_sid123");
    }

    #[test]
    fn legacy_query_accepts_numeric_boolean_connection_settings() {
        let query = SignalQuery {
            access_token: None,
            join_request: None,
            reconnect: None,
            sid: None,
            auto_subscribe: Some("0".to_string()),
            auto_subscribe_data_track: Some("1".to_string()),
            attributes: None,
            protocol: None,
            sdk: None,
            os: None,
            device_model: None,
        };

        let request = legacy_join_request_from_query(&query);
        let settings = request
            .connection_settings
            .expect("connection settings should be present");
        assert!(!settings.auto_subscribe);
        assert_eq!(settings.auto_subscribe_data_track, Some(true));
    }
}
