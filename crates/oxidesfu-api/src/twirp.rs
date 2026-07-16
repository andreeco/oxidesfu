use axum::{
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
};
use prost::Message;
use serde::{Serialize, de::DeserializeOwned};

pub(crate) const APPLICATION_PROTOBUF: &str = "application/protobuf";
pub(crate) const APPLICATION_JSON: &str = "application/json";
pub(crate) const ROOM_SERVICE_PREFIX: &str = "/twirp/livekit.RoomService";
pub(crate) const AGENT_DISPATCH_SERVICE_PREFIX: &str = "/twirp/livekit.AgentDispatchService";
pub(crate) const EGRESS_SERVICE_PREFIX: &str = "/twirp/livekit.Egress";
pub(crate) const INGRESS_SERVICE_PREFIX: &str = "/twirp/livekit.Ingress";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum TwirpCodec {
    Protobuf,
    Json,
}

pub(crate) fn request_codec(headers: &HeaderMap, body: &[u8]) -> TwirpCodec {
    let content_type = headers
        .get(header::CONTENT_TYPE)
        .and_then(|value| value.to_str().ok())
        .unwrap_or_default()
        .to_ascii_lowercase();
    if content_type.contains("json") {
        return TwirpCodec::Json;
    }

    if body
        .iter()
        .copied()
        .find(|byte| !byte.is_ascii_whitespace())
        .is_some_and(|byte| byte == b'{' || byte == b'[')
    {
        return TwirpCodec::Json;
    }

    TwirpCodec::Protobuf
}

pub(crate) fn decode<T>(codec: TwirpCodec, body: &[u8]) -> Result<T, Box<Response>>
where
    T: Message + Default + DeserializeOwned,
{
    match codec {
        TwirpCodec::Protobuf => T::decode(body),
        TwirpCodec::Json => {
            serde_json::from_slice(body).map_err(|error| prost::DecodeError::new(error.to_string()))
        }
    }
    .map_err(|_| {
        Box::new(twirp_error(
            StatusCode::BAD_REQUEST,
            "malformed",
            "the request body could not be decoded",
        ))
    })
}

pub(crate) fn encode<T>(codec: TwirpCodec, message: &T) -> Response
where
    T: Message + Serialize,
{
    match codec {
        TwirpCodec::Protobuf => protobuf_bytes(message.encode_to_vec()),
        TwirpCodec::Json => json_bytes(
            serde_json::to_vec(message)
                .expect("pbjson-backed livekit protocol messages should serialize to JSON"),
        ),
    }
}

pub(crate) fn protobuf_bytes(bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, APPLICATION_PROTOBUF)],
        bytes,
    )
        .into_response()
}

pub(crate) fn json_bytes(bytes: Vec<u8>) -> Response {
    (
        StatusCode::OK,
        [(header::CONTENT_TYPE, APPLICATION_JSON)],
        bytes,
    )
        .into_response()
}

pub(crate) fn twirp_error(status: StatusCode, code: &'static str, msg: &str) -> Response {
    #[derive(Serialize)]
    struct ErrorBody<'a> {
        code: &'static str,
        msg: &'a str,
    }

    (status, axum::Json(ErrorBody { code, msg })).into_response()
}

pub(crate) fn twirp_error_owned(status: StatusCode, code: String, msg: String) -> Response {
    #[derive(Serialize)]
    struct ErrorBody {
        code: String,
        msg: String,
    }

    (status, axum::Json(ErrorBody { code, msg })).into_response()
}
