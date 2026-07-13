use livekit_protocol as proto;

pub(crate) fn publish_data_track_request_response(
    reason: proto::request_response::Reason,
    message: &str,
    request: proto::PublishDataTrackRequest,
) -> proto::SignalResponse {
    proto::SignalResponse {
        message: Some(proto::signal_response::Message::RequestResponse(
            proto::RequestResponse {
                reason: reason as i32,
                message: message.to_string(),
                request: Some(proto::request_response::Request::PublishDataTrack(request)),
                ..Default::default()
            },
        )),
    }
}
