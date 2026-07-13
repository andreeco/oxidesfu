use super::*;

async fn create_media_peer_connection(
    disable_h264: bool,
) -> oxidesfu_rtc::RtcResult<(oxidesfu_rtc::PeerConnection, oxidesfu_rtc::PeerConnectionEvents)> {
    if disable_h264 {
        oxidesfu_rtc::create_peer_connection_with_events_without_h264().await
    } else {
        oxidesfu_rtc::create_peer_connection_with_events().await
    }
}

#[derive(Clone, Copy, Debug)]
pub(super) enum RawDataTopology {
    V0DualPeerConnection,
    V0SinglePeerConnection,
    V1,
}

impl RawDataTopology {
    pub(super) fn name(self) -> &'static str {
        match self {
            Self::V0DualPeerConnection => "v0",
            Self::V0SinglePeerConnection => "v0-single-peer-connection",
            Self::V1 => "v1",
        }
    }

    fn is_dual_peer_connection(self) -> bool {
        matches!(self, Self::V0DualPeerConnection)
    }

    fn signal_path(self) -> String {
        match self {
            Self::V0DualPeerConnection => {
                "/rtc?protocol=15&auto_subscribe=true&auto_subscribe_data_track=true&sdk=rust"
                    .to_string()
            }
            Self::V0SinglePeerConnection => {
                format!("/rtc?join_request={}", encoded_default_join_request())
            }
            Self::V1 => format!("/rtc/v1?join_request={}", encoded_default_join_request()),
        }
    }
}

pub(super) struct RawDataParticipant {
    _socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    _peers: Vec<oxidesfu_rtc::PeerConnection>,
    pub(super) send_data_channel: oxidesfu_rtc::DataChannel,
    _receive_data_channel: oxidesfu_rtc::DataChannel,
    pub(super) open_rx: tokio::sync::mpsc::UnboundedReceiver<()>,
    pub(super) data_rx: tokio::sync::mpsc::UnboundedReceiver<Vec<u8>>,
}

struct BitrateWindow {
    start: std::time::Instant,
    bytes: usize,
}

struct DataChannelReader {
    target_bps: u32,
    windows: std::collections::VecDeque<BitrateWindow>,
    active: BitrateWindow,
    bytes: usize,
    start: std::time::Instant,
}

impl DataChannelReader {
    const DURATION: Duration = Duration::from_secs(10);
    const WINDOW: Duration = Duration::from_millis(100);

    fn new(target_bps: u32) -> Self {
        let now = std::time::Instant::now();
        Self {
            target_bps,
            windows: std::collections::VecDeque::new(),
            active: BitrateWindow {
                start: now,
                bytes: 0,
            },
            bytes: 0,
            start: now,
        }
    }

    fn add_bytes(&mut self, bytes: usize, now: std::time::Instant) {
        if now.duration_since(self.active.start) >= Self::WINDOW {
            let previous = std::mem::replace(
                &mut self.active,
                BitrateWindow {
                    start: now,
                    bytes: 0,
                },
            );
            self.windows.push_back(previous);

            while self.windows.front().is_some_and(|window| {
                now.duration_since(window.start) > Self::DURATION + Self::WINDOW
            }) {
                if let Some(window) = self.windows.pop_front() {
                    self.bytes = self.bytes.saturating_sub(window.bytes);
                }
            }

            if let Some(window) = self.windows.front() {
                self.start = window.start;
            } else {
                self.start = now;
                self.bytes = 0;
            }
        }

        self.bytes = self.bytes.saturating_add(bytes);
        self.active.bytes = self.active.bytes.saturating_add(bytes);
    }

    fn force_bitrate(&self, now: std::time::Instant) -> u128 {
        let elapsed = now.duration_since(self.start).max(Self::WINDOW).as_millis();
        (self.bytes as u128)
            .saturating_mul(8)
            .saturating_mul(1_000)
            .saturating_div(elapsed)
    }

    async fn read(&mut self, bytes: usize) {
        loop {
            let now = std::time::Instant::now();
            if self.force_bitrate(now) <= u128::from(self.target_bps) {
                self.add_bytes(bytes, now);
                return;
            }

            tokio::time::sleep(Duration::from_millis(10)).await;
            self.add_bytes(0, std::time::Instant::now());
        }
    }
}

fn data_packet_payload_len(bytes: &[u8]) -> usize {
    proto::DataPacket::decode(bytes)
        .ok()
        .and_then(|packet| match packet.value {
            Some(proto::data_packet::Value::User(user)) => Some(user.payload.len()),
            _ => None,
        })
        .unwrap_or(bytes.len())
}

fn media_section_summary(sdp: &str) -> String {
    let mut sections = Vec::new();
    let mut media = None;
    let mut mid = None;
    let mut direction = None;

    for line in sdp.lines() {
        if let Some(value) = line.strip_prefix("m=") {
            if let Some(media) = media.take() {
                sections.push(format!("{media}:{}:{}", mid.unwrap_or("?"), direction.unwrap_or("sendrecv")));
            }
            media = value.split_whitespace().next();
            mid = None;
            direction = None;
        } else if let Some(value) = line.strip_prefix("a=mid:") {
            mid = Some(value);
        } else if matches!(line, "a=sendrecv" | "a=sendonly" | "a=recvonly" | "a=inactive") {
            direction = Some(line.trim_start_matches("a="));
        }
    }
    if let Some(media) = media {
        sections.push(format!("{media}:{}:{}", mid.unwrap_or("?"), direction.unwrap_or("sendrecv")));
    }
    sections.join(",")
}

/// A low-level native media client that preserves LiveKit's three signaling topologies.
///
/// It intentionally owns the websocket and peer-event streams so callers can drive
/// renegotiation while receiving actual RTP rather than publication metadata.
/// A remote track whose codec was resolved from an RTP packet received by the native client.
pub(super) struct ReceivedMediaTrack {
    pub(super) track: oxidesfu_rtc::RemoteTrack,
    pub(super) mime_type: String,
    pub(super) first_rtp_packet: rtc::rtp::Packet,
}

/// An actionable failure from the native same-room media harness.
#[derive(Debug)]
pub(super) enum NativeHarnessError {
    ReceiveTimeout {
        topology: RawDataTopology,
        expected: usize,
        received: usize,
        remote_track_kinds: Vec<String>,
        signal_history: Vec<String>,
    },
    RemoteTrackClosed,
    RtpReceive {
        kind: String,
        detail: String,
    },
    CodecResolution {
        kind: String,
        ssrc: u32,
    },
    LocalRtpWrite(String),
    ActorClosed,
}

impl std::fmt::Display for NativeHarnessError {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Self::ReceiveTimeout {
                topology,
                expected,
                received,
                remote_track_kinds,
                signal_history,
            } => write!(
                formatter,
                "timed out receiving RTP in {}: received {received}/{expected} remote-track events; kinds={remote_track_kinds:?}; signaling={signal_history:?}",
                topology.name(),
            ),
            Self::RemoteTrackClosed => formatter.write_str("remote-track event stream closed"),
            Self::RtpReceive { kind, detail } => {
                write!(formatter, "remote {kind} track did not yield RTP: {detail}")
            }
            Self::CodecResolution { kind, ssrc } => {
                write!(formatter, "remote {kind} track RTP SSRC {ssrc} did not resolve to a codec")
            }
            Self::LocalRtpWrite(detail) => write!(formatter, "failed to write local RTP: {detail}"),
            Self::ActorClosed => formatter.write_str("persistent media participant actor stopped"),
        }
    }
}

impl std::error::Error for NativeHarnessError {}

#[derive(Default)]
struct PublisherNegotiation {
    in_flight_offer_id: Option<u32>,
    queued: bool,
}

impl PublisherNegotiation {
    /// Returns whether a new publisher offer may be created now.
    fn request_offer(&mut self) -> bool {
        if self.in_flight_offer_id.is_some() {
            self.queued = true;
            false
        } else {
            true
        }
    }

    /// Records the identifier of an offer after it is written to the signaling socket.
    fn offer_sent(&mut self, offer_id: u32) {
        assert!(self.in_flight_offer_id.is_none(), "publisher offers must be serialized");
        self.in_flight_offer_id = Some(offer_id);
    }

    /// Completes the matching offer and reports whether a queued offer must follow it.
    fn answer_received(&mut self, offer_id: u32) -> bool {
        if self.in_flight_offer_id != Some(offer_id) {
            return false;
        }
        self.in_flight_offer_id = None;
        std::mem::take(&mut self.queued)
    }

    fn is_idle(&self) -> bool {
        self.in_flight_offer_id.is_none() && !self.queued
    }
}

pub(super) struct NativeMediaParticipant {
    socket: tokio_tungstenite::WebSocketStream<
        tokio_tungstenite::MaybeTlsStream<tokio::net::TcpStream>,
    >,
    topology: RawDataTopology,
    publisher: oxidesfu_rtc::PeerConnection,
    subscriber: Option<oxidesfu_rtc::PeerConnection>,
    publisher_ice_candidates: tokio::sync::mpsc::UnboundedReceiver<oxidesfu_rtc::IceCandidate>,
    subscriber_ice_candidates: Option<tokio::sync::mpsc::UnboundedReceiver<oxidesfu_rtc::IceCandidate>>,
    subscriber_data_channels: Option<tokio::sync::mpsc::UnboundedReceiver<oxidesfu_rtc::DataChannel>>,
    remote_tracks: tokio::sync::mpsc::UnboundedReceiver<oxidesfu_rtc::RemoteTrack>,
    _publisher_data_channel: oxidesfu_rtc::DataChannel,
    publisher_data_track_channel: oxidesfu_rtc::DataChannel,
    subscriber_data_track_channel: Option<oxidesfu_rtc::DataChannel>,
    published_track_sids: std::collections::HashMap<String, String>,
    received_track_metadata: std::collections::VecDeque<proto::TrackInfo>,
    published_data_tracks: std::collections::VecDeque<proto::DataTrackInfo>,
    data_track_subscriber_handles: std::collections::VecDeque<proto::DataTrackSubscriberHandles>,
    subscription_responses: std::collections::VecDeque<proto::SubscriptionResponse>,
    next_offer_id: u32,
    publisher_negotiation: PublisherNegotiation,
    subscriber_offer_in_flight: bool,
    answered_subscriber_offer_ids: std::collections::HashSet<u32>,
    signal_history: std::collections::VecDeque<String>,

}

impl NativeMediaParticipant {
    /// Connects a native client using the specified upstream LiveKit topology.
    pub(super) async fn connect(
        topology: RawDataTopology,
        addr: std::net::SocketAddr,
        room: &str,
        identity: &str,
    ) -> Self {
        Self::connect_with_token_and_h264_disabled(topology, addr, &join_token(room, identity), false, false).await
    }

    /// Connects a native participant using an already-minted access token.
    pub(super) async fn connect_with_token(
        topology: RawDataTopology,
        addr: std::net::SocketAddr,
        token: &str,
    ) -> Self {
        Self::connect_with_token_and_h264_disabled(topology, addr, token, false, false).await
    }

    /// Connects a native participant that does not register H264 as a media capability.
    pub(super) async fn connect_without_h264(
        topology: RawDataTopology,
        addr: std::net::SocketAddr,
        room: &str,
        identity: &str,
    ) -> Self {
        Self::connect_with_token_and_h264_disabled(topology, addr, &join_token(room, identity), true, false).await
    }

    async fn connect_with_token_and_h264_disabled(
        topology: RawDataTopology,
        addr: std::net::SocketAddr,
        token: &str,
        disable_h264: bool,
        wait_for_subscriber_data_channel: bool,
    ) -> Self {
        let (socket, _) = connect_signal_socket_at_path(
            &format!("http://{addr}"),
            token,
            &topology.signal_path(),
        )
        .await;

        let (publisher, publisher_events) = create_media_peer_connection(disable_h264)
            .await
            .expect("publisher peer connection should create");
        let oxidesfu_rtc::PeerConnectionEvents {
            ice_candidates: publisher_ice_candidates,
            data_channels: _,
            remote_tracks: publisher_remote_tracks,
        } = publisher_events;
        let publisher_data_channel = publisher
            .create_data_channel(if topology.is_dual_peer_connection() {
                "pubraw"
            } else {
                "data"
            })
            .await
            .expect("publisher data channel should create");
        let publisher_data_track_channel = publisher
            .create_data_channel("_data_track")
            .await
            .expect("publisher data-track channel should create");
        if !topology.is_dual_peer_connection() {
            // Match the Rust SDK single-PC initialization. Reserving these sections in the
            // initial offer prevents concurrent publication from racing a reactive
            // MediaSectionsRequirement before a remote receiver exists.
            publisher
                .add_recvonly_transceivers(rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio, 3)
                .await
                .expect("single-PC publisher should reserve audio receive sections");
            publisher
                .add_recvonly_transceivers(rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video, 3)
                .await
                .expect("single-PC publisher should reserve video receive sections");
        }

        let (subscriber, subscriber_ice_candidates, subscriber_data_channels, remote_tracks) =
            if topology.is_dual_peer_connection() {
                let (peer, events) = create_media_peer_connection(disable_h264)
                    .await
                    .expect("subscriber peer connection should create");
                let oxidesfu_rtc::PeerConnectionEvents {
                    ice_candidates,
                    data_channels,
                    remote_tracks,
                } = events;
                (Some(peer), Some(ice_candidates), Some(data_channels), remote_tracks)
            } else {
                (None, None, None, publisher_remote_tracks)
            };

        let mut client = Self {
            socket,
            topology,
            publisher,
            subscriber,
            publisher_ice_candidates,
            subscriber_ice_candidates,
            subscriber_data_channels,
            remote_tracks,
            _publisher_data_channel: publisher_data_channel,
            publisher_data_track_channel,
            subscriber_data_track_channel: None,
            published_track_sids: std::collections::HashMap::new(),
            received_track_metadata: std::collections::VecDeque::new(),
            published_data_tracks: std::collections::VecDeque::new(),
            data_track_subscriber_handles: std::collections::VecDeque::new(),
            subscription_responses: std::collections::VecDeque::new(),
            next_offer_id: 10,
            publisher_negotiation: PublisherNegotiation::default(),
            subscriber_offer_in_flight: false,
            answered_subscriber_offer_ids: std::collections::HashSet::new(),
            signal_history: std::collections::VecDeque::new(),

        };
        client
            .complete_initial_negotiation(wait_for_subscriber_data_channel)
            .await;
        client
    }

    async fn complete_initial_negotiation(&mut self, wait_for_subscriber_data_channel: bool) {
        self.send_publisher_offer().await;
        let mut publisher_answered = false;
        let mut subscriber_control_channel_ready = false;
        tokio::time::timeout(Duration::from_secs(10), async {
            while !publisher_answered
                || (wait_for_subscriber_data_channel
                    && self.topology.is_dual_peer_connection()
                    && !subscriber_control_channel_ready)
            {
                tokio::select! {
                    candidate = self.publisher_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            self.send_candidate(candidate, proto::SignalTarget::Publisher).await;
                        }
                    }
                    candidate = async {
                        match self.subscriber_ice_candidates.as_mut() {
                            Some(candidates) => candidates.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if let Some(candidate) = candidate {
                            self.send_candidate(candidate, proto::SignalTarget::Subscriber).await;
                        }
                    }
                    channel = async {
                        match self.subscriber_data_channels.as_mut() {
                            Some(channels) => channels.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if let Some(channel) = channel {
                            let label = channel.label().await.expect("subscriber data channel label should read");
                            if label == "_reliable" || label == "data" {
                                subscriber_control_channel_ready = true;
                            }
                            if label == "_data_track" {
                                self.subscriber_data_track_channel = Some(channel);
                            }
                        }
                    }
                    message = self.socket.next() => {
                        publisher_answered |= self.handle_signal_message(message).await;
                    }
                }
            }
        })
        .await
        .expect("initial SDP and ICE negotiation should complete");
    }

    async fn send_candidate(&mut self, candidate: oxidesfu_rtc::IceCandidate, target: proto::SignalTarget) {
        let request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Trickle(proto::TrickleRequest {
                candidate_init: candidate.candidate_init_json,
                target: target as i32,
                r#final: candidate.is_final,
            })),
        };
        self.socket
            .send(Message::Binary(request.encode_to_vec().into()))
            .await
            .expect("local ICE candidate should send");
    }

    fn record_signal_event(&mut self, event: String) {
        const SIGNAL_HISTORY_LIMIT: usize = 16;
        if self.signal_history.len() == SIGNAL_HISTORY_LIMIT {
            self.signal_history.pop_front();
        }
        self.signal_history.push_back(event);
    }

    async fn send_publisher_offer(&mut self) {
        // SDP offer/answer is strictly serial. In particular, a media-section request can
        // arrive while an earlier publisher offer is awaiting its answer.
        if !self.publisher_negotiation.request_offer() {
            return;
        }

        let offer = self.publisher.create_offer().await.expect("publisher offer should create");
        let offer_id = self.next_offer_id;
        self.record_signal_event(format!(
            "publisher offer {offer_id}: {}",
            media_section_summary(&offer)
        ));
        let request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::Offer(proto::SessionDescription {
                r#type: "offer".to_string(),
                sdp: offer,
                id: offer_id,
                ..Default::default()
            })),
        };
        self.next_offer_id = self.next_offer_id.wrapping_add(1);
        self.publisher_negotiation.offer_sent(offer_id);
        self.socket
            .send(Message::Binary(request.encode_to_vec().into()))
            .await
            .expect("publisher offer should send");
    }

    /// Starts publishing a local RTP track without waiting for signaling acknowledgement.
    pub(super) async fn begin_publish_track(
        &mut self,
        cid: &str,
        name: &str,
        kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
        mime_type: &str,
    ) -> oxidesfu_rtc::LocalRtpTrack {
        let track = self
            .publisher
            .add_local_rtp_track_with_mime(cid, name, kind, mime_type)
            .await
            .expect("native RTP track should add");
        let request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::AddTrack(proto::AddTrackRequest {
                cid: cid.to_string(),
                name: name.to_string(),
                r#type: match kind {
                    rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio => proto::TrackType::Audio as i32,
                    rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video => proto::TrackType::Video as i32,
                    _ => panic!("native media test supports audio and video tracks only"),
                },
                source: if kind == rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video {
                    proto::TrackSource::Camera as i32
                } else {
                    proto::TrackSource::Microphone as i32
                },
                simulcast_codecs: vec![proto::SimulcastCodec {
                    cid: cid.to_string(),
                    codec: mime_type.to_string(),
                    ..Default::default()
                }],
                ..Default::default()
            })),
        };
        self.socket
            .send(Message::Binary(request.encode_to_vec().into()))
            .await
            .expect("add-track request should send before offer");
        track
    }

    /// Publishes a real local RTP track and waits for the publisher negotiation.
    pub(super) async fn publish_track(
        &mut self,
        cid: &str,
        name: &str,
        kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
        mime_type: &str,
    ) -> oxidesfu_rtc::LocalRtpTrack {
        let track = self.begin_publish_track(cid, name, kind, mime_type).await;
        self.wait_for_track_published(cid).await;
        self.send_publisher_offer().await;
        self.wait_for_publisher_answer().await;
        track
    }

    fn data_track_receive_channel(&self) -> &oxidesfu_rtc::DataChannel {
        self.subscriber_data_track_channel
            .as_ref()
            .unwrap_or(&self.publisher_data_track_channel)
    }

    /// Waits until this participant has received publication metadata for `expected` tracks.
    pub(super) async fn wait_for_remote_track_metadata(&mut self, expected: usize) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while self.received_track_metadata.len() < expected {
                self.drive_signal_once().await;
            }
        })
        .await
        .expect("subscriber should receive remote publication metadata");
    }

    /// Asserts that no remotely subscribed RTP receiver is created during `duration`.
    pub(super) async fn assert_no_remote_track(&mut self, duration: Duration) {
        let received = tokio::time::timeout(duration, self.remote_tracks.recv()).await;
        assert!(
            received.is_err(),
            "subscriber without permission must not receive a remote RTP track"
        );
    }

    /// Publishes a LiveKit data track and returns its publisher-side handle.
    pub(super) async fn publish_data_track(&mut self, handle: u16, name: &str) -> proto::DataTrackInfo {
        let request = proto::SignalRequest {
            message: Some(proto::signal_request::Message::PublishDataTrackRequest(
                proto::PublishDataTrackRequest {
                    pub_handle: u32::from(handle),
                    name: name.to_string(),
                    ..Default::default()
                },
            )),
        };
        self.socket
            .send(Message::Binary(request.encode_to_vec().into()))
            .await
            .expect("publish data-track request should send");
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(info) = self.published_data_tracks.pop_front() {
                    break info;
                }
                self.drive_signal_once().await;
            }
        })
        .await
        .expect("published data-track response should arrive")
    }



    /// Waits for the server-assigned subscriber handle for a published data track.
    pub(super) async fn wait_for_data_track_subscription(
        &mut self,
        track_sid: &str,
    ) -> u16 {
        let subscriber_handle = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(handles) = self.data_track_subscriber_handles.pop_front() {
                    if let Some((handle, _)) = handles
                        .sub_handles
                        .into_iter()
                        .find(|(_, track)| track.track_sid == track_sid)
                    {
                        break u16::try_from(handle).expect("subscriber data-track handle should fit u16");
                    }
                }
                self.drive_signal_once().await;
            }
        })
        .await
        .expect("subscriber should receive a data-track handle after permission grant");
        subscriber_handle
    }

    /// Asserts that no data-track frame reaches this participant during `duration`.
    pub(super) async fn assert_no_data_track_frame(&self, duration: Duration) {
        let received = tokio::time::timeout(duration, self.data_track_receive_channel().recv_bytes()).await;
        assert!(
            received.is_err(),
            "subscriber without permission must not receive a data-track frame"
        );
    }

    /// Sends a complete one-packet data-track frame through the native data channel.
    pub(super) async fn send_data_track_frame(&self, handle: u16, payload: &[u8]) {
        let packet = proto::DataPacket {
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                payload: payload.to_vec(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let mut frame = vec![0x38, 0, 0, 0, 0, 1, 0, 1, 0, 0, 0, 1];
        frame[2..4].copy_from_slice(&handle.to_be_bytes());
        frame.extend(packet.encode_to_vec());
        self.publisher_data_track_channel
            .send_bytes(&frame)
            .await
            .expect("data-track frame should send");
    }

    /// Receives a forwarded data-track frame and verifies the server rewrote its handle.
    pub(super) async fn receive_data_track_frame(&self, expected_handle: u16, expected_payload: &[u8]) {
        const DATA_TRACK_HEADER_LENGTH: usize = 12;

        let bytes = tokio::time::timeout(Duration::from_secs(10), self.data_track_receive_channel().recv_bytes())
            .await
            .expect("subscriber should receive data-track bytes")
            .expect("data-track channel should remain open");
        assert!(
            bytes.len() >= DATA_TRACK_HEADER_LENGTH,
            "forwarded data-track frame should include its header"
        );
        assert_eq!(
            u16::from_be_bytes([bytes[2], bytes[3]]),
            expected_handle,
            "server should rewrite the data-track subscriber handle"
        );
        let packet = proto::DataPacket::decode(&bytes[DATA_TRACK_HEADER_LENGTH..])
            .expect("forwarded data packet should use the LiveKit protobuf wire format");
        let Some(proto::data_packet::Value::User(user)) = packet.value else {
            panic!("forwarded data packet should contain a user payload");
        };
        assert_eq!(user.payload, expected_payload, "forwarded data payload should match");
    }

    async fn wait_for_track_published(&mut self, cid: &str) {
        tokio::time::timeout(Duration::from_secs(10), async {
            while !self.published_track_sids.contains_key(cid) {
                self.drive_signal_once().await;
            }
        })
        .await
        .expect("published track should be acknowledged");
    }



    pub(super) fn published_track_sid(&self, cid: &str) -> String {
        self.published_track_sids
            .get(cid)
            .cloned()
            .expect("published track SID should be recorded")
    }

    async fn wait_for_publisher_answer(&mut self) {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = self.publisher_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            self.send_candidate(candidate, proto::SignalTarget::Publisher).await;
                        }
                    }
                    candidate = async {
                        match self.subscriber_ice_candidates.as_mut() {
                            Some(candidates) => candidates.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if let Some(candidate) = candidate {
                            self.send_candidate(candidate, proto::SignalTarget::Subscriber).await;
                        }
                    }
                    message = self.socket.next() => {
                        if self.handle_signal_message(message).await && self.publisher_negotiation.is_idle() {
                            break;
                        }
                    }
                }
            }
        })
        .await
        .expect("publisher renegotiation should answer");
    }

    /// Processes one pending signaling, ICE, or subscriber data-channel event.
    pub(super) async fn drive_signal_once(&mut self) {
        tokio::select! {
            candidate = self.publisher_ice_candidates.recv() => {
                if let Some(candidate) = candidate {
                    self.send_candidate(candidate, proto::SignalTarget::Publisher).await;
                }
            }
            candidate = async {
                match self.subscriber_ice_candidates.as_mut() {
                    Some(candidates) => candidates.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(candidate) = candidate {
                    self.send_candidate(candidate, proto::SignalTarget::Subscriber).await;
                }
            }
            channel = async {
                match self.subscriber_data_channels.as_mut() {
                    Some(channels) => channels.recv().await,
                    None => std::future::pending().await,
                }
            } => {
                if let Some(channel) = channel
                    && channel.label().await.expect("subscriber data channel label should read") == "_data_track"
                {
                    self.subscriber_data_track_channel = Some(channel);
                }
            }
            message = self.socket.next() => {
                let _ = self.handle_signal_message(message).await;
            }
        }
    }

    async fn handle_signal_message(
        &mut self,
        message: Option<Result<Message, tokio_tungstenite::tungstenite::Error>>,
    ) -> bool {
        let Some(Ok(Message::Binary(bytes))) = message else {
            return false;
        };
        let response = proto::SignalResponse::decode(bytes.as_ref()).expect("signal response should decode");
        let response_summary = match response.message.as_ref() {
            Some(proto::signal_response::Message::Answer(answer)) => format!(
                "answer {} mid_to_track_id={:?}: {}",
                answer.id,
                answer.mid_to_track_id,
                media_section_summary(&answer.sdp)
            ),
            Some(proto::signal_response::Message::Offer(offer)) => format!("subscriber offer {}", offer.id),
            Some(proto::signal_response::Message::MediaSectionsRequirement(requirement)) => {
                format!("media requirement audio={} video={}", requirement.num_audios, requirement.num_videos)
            }
            Some(proto::signal_response::Message::TrackPublished(track)) => {
                format!("track published {}", track.cid)
            }
            Some(proto::signal_response::Message::Trickle(trickle)) => {
                format!("trickle target={}", trickle.target)
            }
            Some(_) => "other signal response".to_string(),
            None => "empty signal response".to_string(),
        };
        self.record_signal_event(response_summary);
        match response.message {
            Some(proto::signal_response::Message::Answer(answer)) => {
                // The server can retransmit an answer after the next offer has already been
                // created. Applying it would corrupt the publisher's signaling state.
                if self.publisher_negotiation.in_flight_offer_id != Some(answer.id) {
                    return false;
                }

                self.publisher
                    .set_remote_answer(answer.sdp)
                    .await
                    .expect("publisher answer should apply");
                if self.publisher_negotiation.answer_received(answer.id) {
                    self.send_publisher_offer().await;
                }
                true
            }
            Some(proto::signal_response::Message::TrackPublished(track_published)) => {
                let track = track_published.track.expect("published track response should include track");
                self.published_track_sids.insert(track_published.cid, track.sid);
                false
            }
            Some(proto::signal_response::Message::Update(update)) => {
                self.received_track_metadata.extend(
                    update
                        .participants
                        .into_iter()
                        .flat_map(|participant| participant.tracks),
                );
                false
            }
            Some(proto::signal_response::Message::PublishDataTrackResponse(response)) => {
                self.published_data_tracks.push_back(
                    response.info.expect("published data-track response should include track info"),
                );
                false
            }
            Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) => {
                self.data_track_subscriber_handles.push_back(handles);
                false
            }
            Some(proto::signal_response::Message::SubscriptionResponse(response)) => {
                self.subscription_responses.push_back(response);
                false
            }
            Some(proto::signal_response::Message::Offer(offer)) => {
                assert!(
                    !self.subscriber_offer_in_flight,
                    "server subscriber offers must not overlap"
                );
                if !self.answered_subscriber_offer_ids.insert(offer.id) {
                    return false;
                }

                self.subscriber_offer_in_flight = true;
                let subscriber = self
                    .subscriber
                    .as_ref()
                    .expect("only a dual-PC client should receive a server offer");
                let answer = subscriber
                    .create_answer_for_offer(offer.sdp)
                    .await
                    .expect("subscriber offer should answer");
                let request = proto::SignalRequest {
                    message: Some(proto::signal_request::Message::Answer(proto::SessionDescription {
                        r#type: "answer".to_string(),
                        sdp: answer,
                        id: offer.id,
                        ..Default::default()
                    })),
                };
                self.socket
                    .send(Message::Binary(request.encode_to_vec().into()))
                    .await
                    .expect("subscriber answer should send");
                self.subscriber_offer_in_flight = false;
                false
            }
            Some(proto::signal_response::Message::MediaSectionsRequirement(requirement)) => {
                // Requirements are incremental: each one reserves the requested number of
                // additional receive sections for newly forwarded tracks.
                self.publisher
                    .add_recvonly_transceivers(
                        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Audio,
                        requirement.num_audios,
                    )
                    .await
                    .expect("requested audio receive sections should add");
                self.publisher
                    .add_recvonly_transceivers(
                        rtc::rtp_transceiver::rtp_sender::RtpCodecKind::Video,
                        requirement.num_videos,
                    )
                    .await
                    .expect("requested video receive sections should add");
                self.send_publisher_offer().await;
                false
            }
            Some(proto::signal_response::Message::Trickle(trickle)) => {
                let target = proto::SignalTarget::try_from(trickle.target)
                    .unwrap_or(proto::SignalTarget::Publisher);
                let peer = if target == proto::SignalTarget::Subscriber {
                    self.subscriber
                        .as_ref()
                        .expect("subscriber candidate requires a subscriber peer")
                } else {
                    &self.publisher
                };
                peer.add_ice_candidate_json(&trickle.candidate_init)
                    .await
                    .expect("server ICE candidate should apply");
                false
            }
            _ => false,
        }
    }

    /// Waits for the one codec-rejection response expected for a published track.
    pub(super) async fn wait_for_unsupported_codec(&mut self, track_sid: &str) {
        let response = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                if let Some(response) = self.subscription_responses.pop_front() {
                    break response;
                }
                self.drive_signal_once().await;
            }
        })
        .await
        .expect("unsupported codec subscription response should arrive");

        assert_eq!(response.track_sid, track_sid, "codec rejection should identify the H264 track");
        assert_eq!(
            response.err,
            proto::SubscriptionError::SeCodecUnsupported as i32,
            "H264 subscription should fail with SE_CODEC_UNSUPPORTED"
        );
    }

    /// Asserts that no additional subscription response is emitted during recovery.
    pub(super) async fn assert_no_subscription_response(&mut self, duration: Duration) {
        tokio::time::timeout(duration, async {
            loop {
                if let Some(response) = self.subscription_responses.pop_front() {
                    panic!("unexpected subscription response during recovery: {response:?}");
                }
                self.drive_signal_once().await;
            }
        })
        .await
        .expect_err("no additional subscription response should be emitted");
    }

    /// Receives `expected` newly subscribed tracks, consuming one RTP packet per track.
    ///
    /// The returned MIME values are resolved from each remote track's received RTP SSRC.
    pub(super) async fn receive_tracks(
        &mut self,
        expected: usize,
        local_tracks: &[oxidesfu_rtc::LocalRtpTrack],
    ) -> Vec<ReceivedMediaTrack> {
        self.receive_tracks_detailed(expected, local_tracks)
            .await
            .unwrap_or_else(|error| panic!("subscriber should receive all expected RTP tracks: {error}"))
    }

    /// Receives RTP with failure context suitable for the persistent same-room harness.
    async fn receive_tracks_detailed(
        &mut self,
        expected: usize,
        local_tracks: &[oxidesfu_rtc::LocalRtpTrack],
    ) -> Result<Vec<ReceivedMediaTrack>, NativeHarnessError> {
        let mut received = Vec::with_capacity(expected);
        let mut remote_track_kinds = Vec::new();
        let mut sequence_number = 1_u16;
        let timeout = tokio::time::sleep(Duration::from_secs(15));
        tokio::pin!(timeout);
        while received.len() < expected {
            tokio::select! {
                    candidate = self.publisher_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            self.send_candidate(candidate, proto::SignalTarget::Publisher).await;
                        }
                    }
                    candidate = async {
                        match self.subscriber_ice_candidates.as_mut() {
                            Some(candidates) => candidates.recv().await,
                            None => std::future::pending().await,
                        }
                    } => {
                        if let Some(candidate) = candidate {
                            self.send_candidate(candidate, proto::SignalTarget::Subscriber).await;
                        }
                    }
                    message = self.socket.next() => {
                        let _ = self.handle_signal_message(message).await;
                    }
                    track = self.remote_tracks.recv() => {
                        let track = track.ok_or(NativeHarnessError::RemoteTrackClosed)?;
                        let kind = format!("{:?}", track.kind().await);
                        remote_track_kinds.push(kind.clone());
                        let packet = tokio::time::timeout(Duration::from_secs(8), track.recv_rtp_packet())
                            .await
                            .map_err(|_| NativeHarnessError::RtpReceive {
                                kind: kind.clone(),
                                detail: "timed out after 8 seconds".to_string(),
                            })?
                            .map_err(|error| NativeHarnessError::RtpReceive {
                                kind: kind.clone(),
                                detail: error.to_string(),
                            })?;
                        let mime = track.codec_mime_for_ssrc(packet.header.ssrc).await.ok_or(
                            NativeHarnessError::CodecResolution {
                                kind,
                                ssrc: packet.header.ssrc,
                            },
                        )?;
                        received.push(ReceivedMediaTrack { track, mime_type: mime, first_rtp_packet: packet });
                    }
                    _ = &mut timeout => {
                        return Err(NativeHarnessError::ReceiveTimeout {
                            topology: self.topology,
                            expected,
                            received: received.len(),
                            remote_track_kinds,
                            signal_history: self.signal_history.iter().cloned().collect(),
                        });
                    }
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        for (index, track) in local_tracks.iter().enumerate() {
                            let (payload_type, payload) = match index {
                                0 => (111, bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe])),
                                1 => (96, bytes::Bytes::from_static(&[0x00, 0xff, 0xff, 0xff, 0xff])),
                                _ => (108, bytes::Bytes::from_static(&[0x65, 0x88, 0x84, 0x21])),
                            };
                            track.write_rtp_with_cached_mid(rtc::rtp::Packet {
                                header: rtc::rtp::header::Header {
                                    version: 2,
                                    marker: true,
                                    payload_type,
                                    sequence_number,
                                    timestamp: u32::from(sequence_number) * 3_000,
                                    ssrc: 0x1234_0000_u32.wrapping_add(index as u32),
                                    ..Default::default()
                                },
                                payload,
                            })
                            .await
                            .map_err(|error| NativeHarnessError::LocalRtpWrite(error.to_string()))?;
                        }
                        sequence_number = sequence_number.wrapping_add(1);
                    }
            }
        }
        Ok(received)
    }
}

/// A persistent signaling owner for a native media participant.
///
/// The low-level participant remains intentionally pull-driven for focused tests, while this
/// wrapper is used by same-room scenarios where every participant must keep servicing ICE and
/// server renegotiation while another participant is publishing.
pub(super) struct PersistentMediaParticipant {
    commands: tokio::sync::mpsc::UnboundedSender<MediaParticipantCommand>,
}

enum MediaParticipantCommand {
    PublishTrack {
        cid: String,
        name: String,
        kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
        mime_type: String,
        response: tokio::sync::oneshot::Sender<oxidesfu_rtc::LocalRtpTrack>,
    },
    ReceiveTracks {
        expected: usize,
        local_tracks: Vec<oxidesfu_rtc::LocalRtpTrack>,
        response: tokio::sync::oneshot::Sender<Result<Vec<ReceivedMediaTrack>, NativeHarnessError>>,
    },
}

impl PersistentMediaParticipant {
    /// Connects and starts continuously processing this participant's signaling state.
    pub(super) async fn connect(
        topology: RawDataTopology,
        addr: std::net::SocketAddr,
        room: &str,
        identity: &str,
    ) -> Self {
        Self::spawn(NativeMediaParticipant::connect(topology, addr, room, identity).await)
    }

    fn spawn(mut participant: NativeMediaParticipant) -> Self {
        let (commands, mut command_rx) = tokio::sync::mpsc::unbounded_channel();
        tokio::spawn(async move {
            let mut published_tracks = Vec::new();
            let mut sequence_number = 1_u16;
            loop {
                tokio::select! {
                    command = command_rx.recv() => {
                        let Some(command) = command else {
                            break;
                        };
                        match command {
                            MediaParticipantCommand::PublishTrack { cid, name, kind, mime_type, response } => {
                                let track = participant.publish_track(&cid, &name, kind, &mime_type).await;
                                published_tracks.push(track.clone());
                                let _ = response.send(track);
                            }
                            MediaParticipantCommand::ReceiveTracks { expected, local_tracks, response } => {
                                let tracks = participant.receive_tracks_detailed(expected, &local_tracks).await;
                                let _ = response.send(tracks);
                            }
                        }
                    }
                    _ = participant.drive_signal_once() => {}
                    _ = tokio::time::sleep(Duration::from_millis(10)) => {
                        for (index, track) in published_tracks.iter().enumerate() {
                            let (payload_type, payload) = match index {
                                0 => (111, bytes::Bytes::from_static(&[0xf8, 0xff, 0xfe])),
                                1 => (96, bytes::Bytes::from_static(&[0x00, 0xff, 0xff, 0xff, 0xff])),
                                _ => (108, bytes::Bytes::from_static(&[0x65, 0x88, 0x84, 0x21])),
                            };
                            let _ = track
                                .write_rtp_with_cached_mid(rtc::rtp::Packet {
                                    header: rtc::rtp::header::Header {
                                        version: 2,
                                        marker: true,
                                        payload_type,
                                        sequence_number,
                                        timestamp: u32::from(sequence_number) * 3_000,
                                        ssrc: 0x1234_0000_u32.wrapping_add(index as u32),
                                        ..Default::default()
                                    },
                                    payload,
                                })
                                .await;
                        }
                        sequence_number = sequence_number.wrapping_add(1);
                    }
                }
            }
        });
        Self { commands }
    }

    /// Adds, signals, and waits for a native RTP publication.
    pub(super) async fn publish_track(
        &self,
        cid: &str,
        name: &str,
        kind: rtc::rtp_transceiver::rtp_sender::RtpCodecKind,
        mime_type: &str,
    ) -> oxidesfu_rtc::LocalRtpTrack {
        let (response, receiver) = tokio::sync::oneshot::channel();
        self.commands
            .send(MediaParticipantCommand::PublishTrack {
                cid: cid.to_string(),
                name: name.to_string(),
                kind,
                mime_type: mime_type.to_string(),
                response,
            })
            .expect("persistent media participant should remain running");
        receiver
            .await
            .expect("persistent media participant should respond to publication")
    }

    /// Receives RTP tracks while the actor continues coordinating the other participant.
    pub(super) async fn receive_tracks(
        &self,
        expected: usize,
        local_tracks: Vec<oxidesfu_rtc::LocalRtpTrack>,
    ) -> Result<Vec<ReceivedMediaTrack>, NativeHarnessError> {
        let (response, receiver) = tokio::sync::oneshot::channel();
        self.commands
            .send(MediaParticipantCommand::ReceiveTracks {
                expected,
                local_tracks,
                response,
            })
            .map_err(|_| NativeHarnessError::ActorClosed)?;
        receiver.await.map_err(|_| NativeHarnessError::ActorClosed)?
    }
}

#[cfg(test)]
mod negotiation_tests {
    use super::PublisherNegotiation;

    #[test]
    fn matching_answer_completes_the_in_flight_offer() {
        let mut negotiation = PublisherNegotiation::default();
        assert!(negotiation.request_offer());
        negotiation.offer_sent(10);

        assert!(!negotiation.answer_received(9));
        assert!(!negotiation.is_idle());
        assert!(!negotiation.answer_received(10));
        assert!(negotiation.is_idle());
    }

    #[test]
    fn request_during_an_offer_queues_exactly_one_follow_up_offer() {
        let mut negotiation = PublisherNegotiation::default();
        negotiation.offer_sent(10);

        assert!(!negotiation.request_offer());
        assert!(!negotiation.request_offer());
        assert!(negotiation.answer_received(10));
        assert!(negotiation.is_idle());

        negotiation.offer_sent(11);
        assert!(!negotiation.answer_received(11));
        assert!(negotiation.is_idle());
    }

    #[test]
    #[should_panic(expected = "publisher offers must be serialized")]
    fn second_offer_cannot_be_recorded_before_the_first_answer() {
        let mut negotiation = PublisherNegotiation::default();
        negotiation.offer_sent(10);
        negotiation.offer_sent(11);
    }
}

pub(super) async fn connect_raw_data_participant(
    topology: RawDataTopology,
    addr: std::net::SocketAddr,
    room: &str,
    identity: &str,
    target_read_bitrate_bps: Option<u32>,
    drain_after_gap: Option<Arc<std::sync::atomic::AtomicBool>>,
    sender_block_write: bool,
) -> RawDataParticipant {
    let token = join_token(room, identity);
    let (mut socket, _participant_sid) = connect_signal_socket_at_path(
        &format!("http://{addr}"),
        &token,
        &topology.signal_path(),
    )
    .await;

    let (publisher, mut publisher_events) = oxidesfu_rtc::create_peer_connection_with_events_with_transport_and_data_channel_options(
        &oxidesfu_rtc::RtcTransportConfig::default(),
        sender_block_write,
        None,
    )
    .await
    .expect("publisher peer connection should create");
    // In v0 dual-PC mode, keep the publisher's input channel distinct from the
    // server-created subscriber `_reliable` channel. The server selects the latter
    // for downstream fan-out; `pubraw` is still classified as a reliable input.
    let publisher_data_channel = publisher
        .create_data_channel(if topology.is_dual_peer_connection() {
            "pubraw"
        } else {
            "data"
        })
        .await
        .expect("publisher data channel should create");

    let mut subscriber = None;
    let mut subscriber_ice_candidates = None;
    let mut subscriber_data_channels = None;
    if topology.is_dual_peer_connection() {
        let (peer, events) = oxidesfu_rtc::create_peer_connection_with_events_with_transport_and_data_channel_options(
            &oxidesfu_rtc::RtcTransportConfig::default(),
            false,
            None,
        )
        .await
        .expect("subscriber peer connection should create");
        subscriber = Some(peer);
        subscriber_ice_candidates = Some(events.ice_candidates);
        subscriber_data_channels = Some(events.data_channels);
    }

    let offer = proto::SignalRequest {
        message: Some(proto::signal_request::Message::Offer(proto::SessionDescription {
            r#type: "offer".to_string(),
            sdp: publisher.create_offer().await.expect("offer should create"),
            id: 10,
            ..Default::default()
        })),
    };
    socket
        .send(Message::Binary(offer.encode_to_vec().into()))
        .await
        .expect("publisher offer should send");

    let mut publisher_answered = false;
    let mut subscriber_data_channel = None;
    tokio::time::timeout(Duration::from_secs(10), async {
        while !publisher_answered
            || (topology.is_dual_peer_connection() && subscriber_data_channel.is_none())
        {
            tokio::select! {
                candidate = publisher_events.ice_candidates.recv() => {
                    if let Some(candidate) = candidate {
                        let request = proto::SignalRequest {
                            message: Some(proto::signal_request::Message::Trickle(proto::TrickleRequest {
                                candidate_init: candidate.candidate_init_json,
                                target: proto::SignalTarget::Publisher as i32,
                                r#final: candidate.is_final,
                            })),
                        };
                        socket.send(Message::Binary(request.encode_to_vec().into())).await
                            .expect("publisher candidate should send");
                    }
                }
                candidate = async {
                    match subscriber_ice_candidates.as_mut() {
                        Some(candidates) => candidates.recv().await,
                        None => std::future::pending().await,
                    }
                } => {
                    if let Some(candidate) = candidate {
                        let request = proto::SignalRequest {
                            message: Some(proto::signal_request::Message::Trickle(proto::TrickleRequest {
                                candidate_init: candidate.candidate_init_json,
                                target: proto::SignalTarget::Subscriber as i32,
                                r#final: candidate.is_final,
                            })),
                        };
                        socket.send(Message::Binary(request.encode_to_vec().into())).await
                            .expect("subscriber candidate should send");
                    }
                }
                channel = async {
                    match subscriber_data_channels.as_mut() {
                        Some(channels) => channels.recv().await,
                        None => std::future::pending().await,
                    }
                }, if subscriber_data_channel.is_none() => {
                    if let Some(channel) = channel {
                        let label = channel.label().await.expect("subscriber data channel label should read");
                        if label == "_reliable" || label == "data" {
                            subscriber_data_channel = Some(channel);
                        }
                    }
                }
                message = socket.next() => {
                    let Some(Ok(Message::Binary(bytes))) = message else {
                        continue;
                    };
                    let response = proto::SignalResponse::decode(bytes.as_ref())
                        .expect("signal response should decode");
                    match response.message {
                        Some(proto::signal_response::Message::Answer(answer)) => {
                            publisher.set_remote_answer(answer.sdp).await
                                .expect("publisher answer should apply");
                            publisher_answered = true;
                        }
                        Some(proto::signal_response::Message::Offer(offer)) => {
                            let subscriber = subscriber.as_ref()
                                .expect("only a dual-PC client should receive a server offer");
                            let answer = subscriber.create_answer_for_offer(offer.sdp).await
                                .expect("subscriber offer should answer");
                            let request = proto::SignalRequest {
                                message: Some(proto::signal_request::Message::Answer(proto::SessionDescription {
                                    r#type: "answer".to_string(),
                                    sdp: answer,
                                    id: offer.id,
                                    ..Default::default()
                                })),
                            };
                            socket.send(Message::Binary(request.encode_to_vec().into())).await
                                .expect("subscriber answer should send");
                        }
                        Some(proto::signal_response::Message::Trickle(trickle)) => {
                            let target = proto::SignalTarget::try_from(trickle.target)
                                .unwrap_or(proto::SignalTarget::Publisher);
                            let peer = if target == proto::SignalTarget::Subscriber {
                                subscriber.as_ref().expect("subscriber candidate requires subscriber peer")
                            } else {
                                &publisher
                            };
                            peer.add_ice_candidate_json(&trickle.candidate_init).await
                                .expect("server candidate should apply");
                        }
                        _ => {}
                    }
                }
            }
        }
    })
    .await
    .expect("SDP and ICE negotiation should complete");

    let receive_data_channel = subscriber_data_channel.unwrap_or_else(|| publisher_data_channel.clone());
    receive_data_channel
        .wait_open()
        .await
        .expect("data channel should open after SDP/trickle exchange");

    let data_channel_for_read = receive_data_channel.clone();
    let (open_tx, open_rx) = tokio::sync::mpsc::unbounded_channel();
    let (data_tx, data_rx) = tokio::sync::mpsc::unbounded_channel();
    tokio::spawn(async move {
        let _ = open_tx.send(());
        let mut reader = target_read_bitrate_bps.map(DataChannelReader::new);

        while let Ok(bytes) = data_channel_for_read.recv_bytes().await {
            let should_throttle = drain_after_gap
                .as_ref()
                .is_none_or(|detected_gap| !detected_gap.load(std::sync::atomic::Ordering::Relaxed));
            if should_throttle {
                if let Some(reader) = reader.as_mut() {
                    reader.read(data_packet_payload_len(&bytes)).await;
                }
            }

            if data_tx.send(bytes).is_err() {
                break;
            }
        }
    });

    let mut peers = vec![publisher];
    if let Some(subscriber) = subscriber {
        peers.push(subscriber);
    }

    RawDataParticipant {
        _socket: socket,
        _peers: peers,
        send_data_channel: publisher_data_channel,
        _receive_data_channel: receive_data_channel,
        open_rx,
        data_rx,
    }
}
