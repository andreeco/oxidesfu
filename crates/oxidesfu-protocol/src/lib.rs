//! LiveKit-compatible protocol helpers used by OxideSFU.
//!
//! OxideSFU keeps its own project identity, but the wire protocol uses LiveKit
//! protobuf messages for compatibility with existing SDKs.

use std::io::Read;

use base64::{Engine, engine::general_purpose};
use flate2::read::GzDecoder;
use livekit_protocol as proto;
use prost::Message;
use thiserror::Error;

/// Maximum decoded join request size, matching Go's `http.DefaultMaxHeaderBytes`.
pub const MAX_JOIN_REQUEST_BYTES: usize = 1 << 20;

/// Errors that can occur while decoding a LiveKit-compatible join request.
#[derive(Debug, Error, PartialEq, Eq)]
pub enum JoinRequestDecodeError {
    /// The outer base64url value could not be decoded.
    #[error("cannot base64 decode wrapped join request")]
    Base64,
    /// The outer protobuf wrapper could not be decoded.
    #[error("cannot unmarshal wrapped join request")]
    WrappedProtobuf,
    /// The inner join request uses an unknown compression enum value.
    #[error("unknown wrapped join request compression: {0}")]
    UnknownCompression(i32),
    /// The uncompressed join request exceeds the compatibility size limit.
    #[error("join request too large")]
    TooLarge,
    /// The gzip stream could not be read.
    #[error("cannot read decompressed join request")]
    GzipRead,
    /// The inner protobuf join request could not be decoded.
    #[error("cannot unmarshal join request")]
    JoinProtobuf,
}

/// Decodes the `/rtc/v1` `join_request` query parameter into a [`proto::JoinRequest`].
pub fn decode_join_request_param(
    param: &str,
) -> Result<proto::JoinRequest, JoinRequestDecodeError> {
    let wrapped_bytes = decode_base64_url(param)?;
    let wrapped = proto::WrappedJoinRequest::decode(wrapped_bytes.as_slice())
        .map_err(|_| JoinRequestDecodeError::WrappedProtobuf)?;

    let join_request_bytes =
        match proto::wrapped_join_request::Compression::try_from(wrapped.compression) {
            Ok(proto::wrapped_join_request::Compression::None) => {
                ensure_size(wrapped.join_request.len())?;
                wrapped.join_request
            }
            Ok(proto::wrapped_join_request::Compression::Gzip) => {
                decompress_gzip_limited(&wrapped.join_request)?
            }
            Err(_) => {
                return Err(JoinRequestDecodeError::UnknownCompression(
                    wrapped.compression,
                ));
            }
        };

    proto::JoinRequest::decode(join_request_bytes.as_slice())
        .map_err(|_| JoinRequestDecodeError::JoinProtobuf)
}

fn decode_base64_url(param: &str) -> Result<Vec<u8>, JoinRequestDecodeError> {
    general_purpose::URL_SAFE
        .decode(param)
        .or_else(|_| general_purpose::URL_SAFE_NO_PAD.decode(param))
        .map_err(|_| JoinRequestDecodeError::Base64)
}

fn decompress_gzip_limited(compressed: &[u8]) -> Result<Vec<u8>, JoinRequestDecodeError> {
    let mut decoder = GzDecoder::new(compressed);
    let mut limited = decoder.by_ref().take((MAX_JOIN_REQUEST_BYTES + 1) as u64);
    let mut out = Vec::new();
    limited
        .read_to_end(&mut out)
        .map_err(|_| JoinRequestDecodeError::GzipRead)?;
    ensure_size(out.len())?;
    Ok(out)
}

fn ensure_size(size: usize) -> Result<(), JoinRequestDecodeError> {
    if size > MAX_JOIN_REQUEST_BYTES {
        return Err(JoinRequestDecodeError::TooLarge);
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use std::{collections::HashMap, io::Write};

    use super::*;
    use flate2::{Compression, write::GzEncoder};

    fn compress_bytes(payload: &[u8]) -> Vec<u8> {
        let mut encoder = GzEncoder::new(Vec::new(), Compression::best());
        encoder
            .write_all(payload)
            .expect("gzip write should succeed");
        encoder.finish().expect("gzip finish should succeed")
    }

    fn wrap_join_request(
        join_request: proto::JoinRequest,
        compression: proto::wrapped_join_request::Compression,
    ) -> String {
        let join_bytes = join_request.encode_to_vec();
        let join_request = match compression {
            proto::wrapped_join_request::Compression::None => join_bytes,
            proto::wrapped_join_request::Compression::Gzip => {
                let mut encoder = GzEncoder::new(Vec::new(), Compression::default());
                encoder
                    .write_all(&join_bytes)
                    .expect("gzip write should succeed");
                encoder.finish().expect("gzip finish should succeed")
            }
        };
        let wrapped = proto::WrappedJoinRequest {
            compression: compression as i32,
            join_request,
        };
        general_purpose::URL_SAFE.encode(wrapped.encode_to_vec())
    }

    fn sample_join_request() -> proto::JoinRequest {
        proto::JoinRequest {
            metadata: "participant metadata".to_string(),
            participant_attributes: HashMap::from([("role".to_string(), "speaker".to_string())]),
            connection_settings: Some(proto::ConnectionSettings {
                auto_subscribe: true,
                adaptive_stream: false,
                ..Default::default()
            }),
            ..Default::default()
        }
    }

    #[test]
    fn decodes_uncompressed_wrapped_join_request() {
        let encoded = wrap_join_request(
            sample_join_request(),
            proto::wrapped_join_request::Compression::None,
        );

        let decoded = decode_join_request_param(&encoded).expect("join request should decode");

        assert_eq!(decoded.metadata, "participant metadata");
        assert_eq!(
            decoded.participant_attributes.get("role"),
            Some(&"speaker".to_string())
        );
        assert!(
            decoded
                .connection_settings
                .expect("settings should exist")
                .auto_subscribe
        );
    }

    #[test]
    fn decodes_gzip_wrapped_join_request() {
        let encoded = wrap_join_request(
            sample_join_request(),
            proto::wrapped_join_request::Compression::Gzip,
        );

        let decoded = decode_join_request_param(&encoded).expect("gzip join request should decode");

        assert_eq!(decoded.metadata, "participant metadata");
    }

    #[test]
    fn decompress_gzip_limited_accepts_small_payload() {
        let compressed = compress_bytes(b"hello world");
        let out = decompress_gzip_limited(&compressed).expect("small gzip payload should decode");
        assert_eq!(out, b"hello world");
    }

    #[test]
    fn decompress_gzip_limited_accepts_payload_exactly_at_cap() {
        let raw = vec![0u8; MAX_JOIN_REQUEST_BYTES];
        let compressed = compress_bytes(&raw);
        let out =
            decompress_gzip_limited(&compressed).expect("payload exactly at cap should decode");
        assert_eq!(out.len(), MAX_JOIN_REQUEST_BYTES);
    }

    #[test]
    fn decompress_gzip_limited_rejects_payload_one_byte_over_cap() {
        let raw = vec![0u8; MAX_JOIN_REQUEST_BYTES + 1];
        let compressed = compress_bytes(&raw);
        let err = decompress_gzip_limited(&compressed).expect_err("payload over cap should fail");
        assert_eq!(err, JoinRequestDecodeError::TooLarge);
    }

    #[test]
    fn decompress_gzip_limited_rejects_decompression_bomb() {
        let raw = vec![0u8; 100 << 20]; // 100 MiB of zeros.
        let compressed = compress_bytes(&raw);
        assert!(
            compressed.len() < (1 << 20),
            "sanity: bomb input should compress dramatically"
        );

        let err = decompress_gzip_limited(&compressed)
            .expect_err("decompression bomb should fail size cap");
        assert_eq!(err, JoinRequestDecodeError::TooLarge);
    }

    #[test]
    fn decompress_gzip_limited_reports_malformed_gzip() {
        let err = decompress_gzip_limited(b"not gzip data")
            .expect_err("malformed gzip payload should fail");
        assert_eq!(err, JoinRequestDecodeError::GzipRead);
    }

    #[test]
    fn rejects_unknown_compression() {
        let wrapped = proto::WrappedJoinRequest {
            compression: 99,
            join_request: sample_join_request().encode_to_vec(),
        };
        let encoded = general_purpose::URL_SAFE.encode(wrapped.encode_to_vec());

        let err = decode_join_request_param(&encoded).expect_err("unknown compression should fail");

        assert_eq!(err, JoinRequestDecodeError::UnknownCompression(99));
    }
}
