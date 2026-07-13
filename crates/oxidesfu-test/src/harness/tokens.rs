    fn join_request_param() -> String {
        let wrapped = proto::WrappedJoinRequest {
            compression: proto::wrapped_join_request::Compression::None as i32,
            join_request: proto::JoinRequest::default().encode_to_vec(),
        };
        general_purpose::URL_SAFE.encode(wrapped.encode_to_vec())
    }
    fn reconnect_join_request_param(
        participant_sid: &str,
        reconnect_reason: proto::ReconnectReason,
    ) -> String {
        let join_request = proto::JoinRequest {
            reconnect: true,
            reconnect_reason: reconnect_reason as i32,
            participant_sid: participant_sid.to_string(),
            ..Default::default()
        };
        let wrapped = proto::WrappedJoinRequest {
            compression: proto::wrapped_join_request::Compression::None as i32,
            join_request: join_request.encode_to_vec(),
        };
        general_purpose::URL_SAFE.encode(wrapped.encode_to_vec())
    }
    fn invalid_gzip_join_request_param() -> String {
        let wrapped = proto::WrappedJoinRequest {
            compression: proto::wrapped_join_request::Compression::Gzip as i32,
            join_request: vec![0xff, 0x00, 0x13, 0x7f],
        };
        general_purpose::URL_SAFE.encode(wrapped.encode_to_vec())
    }
    fn unique_suffix() -> u128 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_millis()
    }
