pub use crate::router::router;

#[cfg(test)]
mod tests {
    use std::{
        sync::{Arc, Mutex, OnceLock},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use async_trait::async_trait;

    use axum::{
        Router,
        body::{Body, Bytes},
        http::{Request, StatusCode, header},
        response::Response,
    };
    use http_body_util::BodyExt;
    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use livekit_protocol as proto;
    use oxidesfu_auth::{ApiKeyStore, AuthContext, Claims, TokenVerifier, VideoGrants};
    use oxidesfu_room::{RoomStore, RoomStoreError};
    use prost::Message;
    use tower::ServiceExt;

    use super::*;
    use crate::{
        errors::optional_auth_middleware,
        state::ApiState,
        twirp::{APPLICATION_JSON, APPLICATION_PROTOBUF},
    };

    const API_KEY: &str = "devkey";
    const API_SECRET: &str = "secret";

    #[derive(Debug, Clone, PartialEq, Eq)]
    enum RuntimeCall {
        ApplyUpdateSubscriptions {
            room: String,
            identity: String,
            subscribe: bool,
            track_sids: Vec<String>,
            participant_tracks: usize,
        },
        DisconnectParticipant {
            room: String,
            identity: String,
            reason: proto::DisconnectReason,
        },
        DisconnectRoomParticipants {
            room: String,
            reason: proto::DisconnectReason,
        },
        RoomDeleted {
            room: String,
        },
        BroadcastParticipantUpdate {
            room: String,
            identity: String,
        },
        PerformRpc {
            room: String,
            destination_identity: String,
            method: String,
            payload: String,
            response_timeout_ms: u32,
        },
    }

    #[derive(Debug, Default)]
    struct RecordingRuntime {
        calls: Mutex<Vec<RuntimeCall>>,
        rpc_result: Mutex<Option<Result<proto::PerformRpcResponse, RoomStoreError>>>,
    }

    #[async_trait]
    impl crate::state::MediaSubscriptionRuntime for RecordingRuntime {
        async fn apply_update_subscriptions(
            &self,
            room: &str,
            identity: &str,
            track_sids: &[String],
            participant_tracks: &[proto::ParticipantTracks],
            subscribe: bool,
        ) {
            self.calls
                .lock()
                .expect("runtime calls lock should not be poisoned")
                .push(RuntimeCall::ApplyUpdateSubscriptions {
                    room: room.to_string(),
                    identity: identity.to_string(),
                    subscribe,
                    track_sids: track_sids.to_vec(),
                    participant_tracks: participant_tracks.len(),
                });
        }

        async fn disconnect_participant(
            &self,
            room: &str,
            identity: &str,
            reason: proto::DisconnectReason,
        ) -> Result<(), RoomStoreError> {
            self.calls
                .lock()
                .expect("runtime calls lock should not be poisoned")
                .push(RuntimeCall::DisconnectParticipant {
                    room: room.to_string(),
                    identity: identity.to_string(),
                    reason,
                });
            Ok(())
        }

        async fn disconnect_room_participants(
            &self,
            room: &str,
            reason: proto::DisconnectReason,
        ) -> Result<(), RoomStoreError> {
            self.calls
                .lock()
                .expect("runtime calls lock should not be poisoned")
                .push(RuntimeCall::DisconnectRoomParticipants {
                    room: room.to_string(),
                    reason,
                });
            Ok(())
        }

        async fn broadcast_participant_update(
            &self,
            room: &str,
            participant: proto::ParticipantInfo,
        ) {
            self.calls
                .lock()
                .expect("runtime calls lock should not be poisoned")
                .push(RuntimeCall::BroadcastParticipantUpdate {
                    room: room.to_string(),
                    identity: participant.identity,
                });
        }

        async fn perform_rpc(
            &self,
            room: &str,
            request: &proto::PerformRpcRequest,
        ) -> Result<proto::PerformRpcResponse, RoomStoreError> {
            self.calls
                .lock()
                .expect("runtime calls lock should not be poisoned")
                .push(RuntimeCall::PerformRpc {
                    room: room.to_string(),
                    destination_identity: request.destination_identity.clone(),
                    method: request.method.clone(),
                    payload: request.payload.clone(),
                    response_timeout_ms: request.response_timeout_ms,
                });
            self.rpc_result
                .lock()
                .expect("runtime rpc_result lock should not be poisoned")
                .take()
                .unwrap_or_else(|| {
                    Ok(proto::PerformRpcResponse {
                        payload: String::new(),
                    })
                })
        }

        async fn room_deleted(&self, room: proto::Room) {
            self.calls
                .lock()
                .expect("runtime calls lock should not be poisoned")
                .push(RuntimeCall::RoomDeleted { room: room.name });
        }
    }

    fn test_state() -> ApiState {
        test_state_with_runtime(None)
    }

    fn test_state_with_runtime(
        media_subscription_runtime: Option<Arc<dyn crate::state::MediaSubscriptionRuntime>>,
    ) -> ApiState {
        let mut keys = ApiKeyStore::new();
        keys.insert(API_KEY, API_SECRET);
        ApiState {
            rooms: RoomStore::default(),
            auth: TokenVerifier::new(keys),
            data_channels: oxidesfu_rtc::DataChannelStore::default(),
            media_subscription_runtime,
            room_service_forwarder: None,
            enable_remote_unmute: false,
        }
    }

    fn token(video: VideoGrants) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("system clock should be after epoch")
            .as_secs() as usize;
        let claims = Claims {
            iss: API_KEY.to_string(),
            exp: now + Duration::from_secs(60).as_secs() as usize,
            video,
            ..Default::default()
        };
        jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret(API_SECRET.as_bytes()),
        )
        .expect("test token should encode")
    }

    async fn post<M: Message>(
        app: Router,
        path: &str,
        bearer: Option<String>,
        message: M,
    ) -> Response {
        let auth_header = bearer.map(|token| format!("Bearer {token}"));
        post_with_authorization_header(app, path, auth_header.as_deref(), message).await
    }

    async fn post_with_authorization_header<M: Message>(
        app: Router,
        path: &str,
        authorization_header: Option<&str>,
        message: M,
    ) -> Response {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header(header::CONTENT_TYPE, APPLICATION_PROTOBUF);
        if let Some(header_value) = authorization_header {
            builder = builder.header(header::AUTHORIZATION, header_value);
        }
        app.oneshot(
            builder
                .body(Body::from(message.encode_to_vec()))
                .expect("request should build"),
        )
        .await
        .expect("router should handle request")
    }

    async fn post_json_with_authorization_header<T: serde::Serialize>(
        app: Router,
        path: &str,
        authorization_header: Option<&str>,
        payload: &T,
    ) -> Response {
        let mut builder = Request::builder()
            .method("POST")
            .uri(path)
            .header(header::CONTENT_TYPE, APPLICATION_JSON);
        if let Some(header_value) = authorization_header {
            builder = builder.header(header::AUTHORIZATION, header_value);
        }
        app.oneshot(
            builder
                .body(Body::from(
                    serde_json::to_vec(payload).expect("json payload should encode"),
                ))
                .expect("request should build"),
        )
        .await
        .expect("router should handle request")
    }

    async fn body_bytes(response: Response) -> Bytes {
        response
            .into_body()
            .collect()
            .await
            .expect("body should collect")
            .to_bytes()
    }

    fn ensure_rustls_crypto_provider() {
        static RUSTLS_PROVIDER_INIT: OnceLock<()> = OnceLock::new();
        let _ = RUSTLS_PROVIDER_INIT.get_or_init(|| {
            let _ = rustls::crypto::ring::default_provider().install_default();
        });
    }

    async fn connected_data_channel_pair() -> (
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::DataChannel,
        oxidesfu_rtc::DataChannel,
    ) {
        ensure_rustls_crypto_provider();

        let (offerer, offerer_events) = oxidesfu_rtc::create_peer_connection_with_events()
            .await
            .expect("offerer peer connection should create");
        let (answerer, answerer_events) = oxidesfu_rtc::create_peer_connection_with_events()
            .await
            .expect("answerer peer connection should create");

        let oxidesfu_rtc::PeerConnectionEvents {
            ice_candidates: mut offerer_ice_candidates,
            data_channels: _,
            remote_tracks: _,
        } = offerer_events;
        let oxidesfu_rtc::PeerConnectionEvents {
            ice_candidates: mut answerer_ice_candidates,
            data_channels: mut answerer_data_channels,
            remote_tracks: _,
        } = answerer_events;

        let offer_channel = offerer
            .create_data_channel("data")
            .await
            .expect("offerer data channel should create");
        let offer_sdp = offerer.create_offer().await.expect("offer should create");
        let answer_sdp = answerer
            .create_answer_for_offer(offer_sdp)
            .await
            .expect("answer should create");
        offerer
            .set_remote_answer(answer_sdp)
            .await
            .expect("answer should apply to offerer");

        let open_channel = offer_channel.clone();
        let open_task = tokio::spawn(async move { open_channel.wait_open().await });
        let answer_channel_task = tokio::spawn(async move {
            answerer_data_channels
                .recv()
                .await
                .ok_or_else(|| std::io::Error::other("answerer data channel stream ended"))
        });
        tokio::pin!(open_task);
        tokio::pin!(answer_channel_task);

        let answer_channel = tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                tokio::select! {
                    candidate = offerer_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            answerer
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("offerer candidate should add to answerer");
                        }
                    }
                    candidate = answerer_ice_candidates.recv() => {
                        if let Some(candidate) = candidate {
                            offerer
                                .add_ice_candidate_json(&candidate.candidate_init_json)
                                .await
                                .expect("answerer candidate should add to offerer");
                        }
                    }
                    result = &mut open_task => {
                        result
                            .expect("open task should not panic")
                            .expect("offerer data channel should open");
                    }
                    result = &mut answer_channel_task => {
                        break result
                            .expect("answer channel task should not panic")
                            .expect("answer data channel should be available");
                    }
                }
            }
        })
        .await
        .expect("data channel should connect before timeout");

        (offerer, answerer, offer_channel, answer_channel)
    }

    async fn recv_packet_with_timeout(
        channel: &oxidesfu_rtc::DataChannel,
        timeout: Duration,
    ) -> Option<proto::DataPacket> {
        match tokio::time::timeout(timeout, channel.recv_bytes()).await {
            Ok(Ok(bytes)) => Some(
                proto::DataPacket::decode(bytes.as_slice())
                    .expect("received bytes should decode as data packet"),
            ),
            _ => None,
        }
    }

    async fn assert_twirp_error(response: Response, status: StatusCode, code: &str) {
        assert_eq!(response.status(), status);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], code);
        let msg = body["msg"].as_str().expect("twirp msg should be a string");
        assert!(!msg.is_empty(), "twirp msg should not be empty");
    }

    async fn get(path: &str) -> Response {
        router(test_state())
            .oneshot(
                Request::builder()
                    .method("GET")
                    .uri(path)
                    .body(Body::empty())
                    .expect("request should build"),
            )
            .await
            .expect("router should respond")
    }

    #[tokio::test]
    async fn egress_start_variant_paths_create_egress_records() {
        let auth = Some(token(VideoGrants {
            room_record: true,
            ..Default::default()
        }));

        let response = post(
            router(test_state()),
            "/twirp/livekit.Egress/StartRoomCompositeEgress",
            auth.clone(),
            proto::RoomCompositeEgressRequest {
                room_name: "room-a".to_string(),
                layout: "speaker-dark".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::EgressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert!(payload.egress_id.starts_with("EG_"));
        assert_eq!(payload.room_name, "room-a");

        let response = post(
            router(test_state()),
            "/twirp/livekit.Egress/StartWebEgress",
            auth.clone(),
            proto::WebEgressRequest {
                url: "https://example.com".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::EgressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert!(payload.egress_id.starts_with("EG_"));

        let response = post(
            router(test_state()),
            "/twirp/livekit.Egress/StartParticipantEgress",
            auth.clone(),
            proto::ParticipantEgressRequest {
                room_name: "room-a".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = post(
            router(test_state()),
            "/twirp/livekit.Egress/StartTrackCompositeEgress",
            auth.clone(),
            proto::TrackCompositeEgressRequest {
                room_name: "room-a".to_string(),
                audio_track_id: "TR_AUDIO".to_string(),
                video_track_id: "TR_VIDEO".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let response = post(
            router(test_state()),
            "/twirp/livekit.Egress/StartTrackEgress",
            auth,
            proto::TrackEgressRequest {
                room_name: "room-a".to_string(),
                track_id: "TR_1".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn list_egress_returns_items_when_room_record_granted() {
        let state = test_state();
        state
            .rooms
            .store_egress_info(&proto::EgressInfo {
                egress_id: "EG_1".to_string(),
                room_name: "room-a".to_string(),
                ..Default::default()
            })
            .expect("egress should store");
        state
            .rooms
            .store_egress_info(&proto::EgressInfo {
                egress_id: "EG_2".to_string(),
                room_name: "room-b".to_string(),
                ..Default::default()
            })
            .expect("egress should store");

        let response = post(
            router(state),
            "/twirp/livekit.Egress/ListEgress",
            Some(token(VideoGrants {
                room_record: true,
                ..Default::default()
            })),
            proto::ListEgressRequest {
                room_name: "room-a".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::ListEgressResponse::decode(body_bytes(response).await).expect("payload decode");
        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].egress_id, "EG_1");
    }

    #[tokio::test]
    async fn list_ingress_returns_items_when_ingress_admin_granted() {
        let state = test_state();
        state
            .rooms
            .store_ingress_info(&proto::IngressInfo {
                ingress_id: "ing-1".to_string(),
                room_name: "room-a".to_string(),
                ..Default::default()
            })
            .expect("ingress should store");
        state
            .rooms
            .store_ingress_info(&proto::IngressInfo {
                ingress_id: "ing-2".to_string(),
                room_name: "room-b".to_string(),
                ..Default::default()
            })
            .expect("ingress should store");

        let response = post(
            router(state),
            "/twirp/livekit.Ingress/ListIngress",
            Some(token(VideoGrants {
                ingress_admin: true,
                ..Default::default()
            })),
            proto::ListIngressRequest {
                room_name: "room-a".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::ListIngressResponse::decode(body_bytes(response).await).expect("payload decode");
        assert_eq!(payload.items.len(), 1);
        assert_eq!(payload.items[0].ingress_id, "ing-1");
    }

    #[tokio::test]
    async fn create_ingress_persists_and_returns_generated_ingress() {
        let response = post(
            router(test_state()),
            "/twirp/livekit.Ingress/CreateIngress",
            Some(token(VideoGrants {
                ingress_admin: true,
                ..Default::default()
            })),
            proto::CreateIngressRequest {
                input_type: proto::IngressInput::WhipInput as i32,
                name: "demo-ingress".to_string(),
                room_name: "room-a".to_string(),
                participant_identity: "publisher-1".to_string(),
                participant_name: "publisher".to_string(),
                participant_metadata: "meta".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::IngressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert!(payload.ingress_id.starts_with("IN_"));
        assert_eq!(payload.name, "demo-ingress");
        assert_eq!(payload.room_name, "room-a");
        assert_eq!(payload.participant_identity, "publisher-1");
        assert_eq!(payload.input_type, proto::IngressInput::WhipInput as i32);
    }

    #[tokio::test]
    async fn update_ingress_updates_existing_record() {
        let state = test_state();
        state
            .rooms
            .store_ingress_info(&proto::IngressInfo {
                ingress_id: "IN_100".to_string(),
                input_type: proto::IngressInput::WhipInput as i32,
                reusable: true,
                name: "old-name".to_string(),
                room_name: "room-a".to_string(),
                participant_identity: "old-identity".to_string(),
                participant_name: "old-name".to_string(),
                ..Default::default()
            })
            .expect("ingress should store");

        let response = post(
            router(state.clone()),
            "/twirp/livekit.Ingress/UpdateIngress",
            Some(token(VideoGrants {
                ingress_admin: true,
                ..Default::default()
            })),
            proto::UpdateIngressRequest {
                ingress_id: "IN_100".to_string(),
                name: "new-name".to_string(),
                room_name: "room-b".to_string(),
                participant_identity: "new-identity".to_string(),
                participant_name: "new-participant".to_string(),
                participant_metadata: "new-meta".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::IngressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert_eq!(payload.ingress_id, "IN_100");
        assert_eq!(payload.name, "new-name");
        assert_eq!(payload.room_name, "room-b");
        assert_eq!(payload.participant_identity, "new-identity");

        let stored = state
            .rooms
            .load_ingress_info("IN_100")
            .expect("updated ingress should persist");
        assert_eq!(stored.name, "new-name");
    }

    #[tokio::test]
    async fn update_ingress_rejects_non_reusable_ingress() {
        let state = test_state();
        state
            .rooms
            .store_ingress_info(&proto::IngressInfo {
                ingress_id: "IN_URL".to_string(),
                input_type: proto::IngressInput::UrlInput as i32,
                reusable: false,
                name: "url-ingress".to_string(),
                ..Default::default()
            })
            .expect("ingress should store");

        let response = post(
            router(state),
            "/twirp/livekit.Ingress/UpdateIngress",
            Some(token(VideoGrants {
                ingress_admin: true,
                ..Default::default()
            })),
            proto::UpdateIngressRequest {
                ingress_id: "IN_URL".to_string(),
                name: "new-name".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "invalid_argument");
        assert_eq!(
            body["msg"],
            "ingress is not reusable and cannot be modified"
        );
    }

    #[tokio::test]
    async fn update_ingress_keeps_completed_ingress_unchanged() {
        let state = test_state();
        state
            .rooms
            .store_ingress_info(&proto::IngressInfo {
                ingress_id: "IN_COMPLETE".to_string(),
                input_type: proto::IngressInput::RtmpInput as i32,
                reusable: true,
                name: "before-name".to_string(),
                room_name: "before-room".to_string(),
                state: Some(proto::IngressState {
                    status: proto::ingress_state::Status::EndpointComplete as i32,
                    ..Default::default()
                }),
                ..Default::default()
            })
            .expect("ingress should store");

        let response = post(
            router(state),
            "/twirp/livekit.Ingress/UpdateIngress",
            Some(token(VideoGrants {
                ingress_admin: true,
                ..Default::default()
            })),
            proto::UpdateIngressRequest {
                ingress_id: "IN_COMPLETE".to_string(),
                name: "after-name".to_string(),
                room_name: "after-room".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::IngressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert_eq!(payload.name, "before-name");
        assert_eq!(payload.room_name, "before-room");
    }

    #[tokio::test]
    async fn create_ingress_url_input_rejects_unsupported_url_scheme() {
        let response = post(
            router(test_state()),
            "/twirp/livekit.Ingress/CreateIngress",
            Some(token(VideoGrants {
                ingress_admin: true,
                ..Default::default()
            })),
            proto::CreateIngressRequest {
                input_type: proto::IngressInput::UrlInput as i32,
                url: "ftp://example.com/stream".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "invalid_argument");
        assert_eq!(body["msg"], "invalid url scheme ftp");
    }

    #[tokio::test]
    async fn delete_ingress_removes_record_and_returns_deleted_info() {
        let state = test_state();
        state
            .rooms
            .store_ingress_info(&proto::IngressInfo {
                ingress_id: "IN_200".to_string(),
                name: "to-delete".to_string(),
                ..Default::default()
            })
            .expect("ingress should store");

        let response = post(
            router(state.clone()),
            "/twirp/livekit.Ingress/DeleteIngress",
            Some(token(VideoGrants {
                ingress_admin: true,
                ..Default::default()
            })),
            proto::DeleteIngressRequest {
                ingress_id: "IN_200".to_string(),
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::IngressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert_eq!(payload.ingress_id, "IN_200");

        assert!(matches!(
            state.rooms.load_ingress_info("IN_200"),
            Err(oxidesfu_room::RoomStoreError::IngressNotFound)
        ));
    }

    #[tokio::test]
    async fn stop_egress_marks_egress_complete() {
        let state = test_state();
        state
            .rooms
            .store_egress_info(&proto::EgressInfo {
                egress_id: "EG_900".to_string(),
                room_name: "room-a".to_string(),
                status: proto::EgressStatus::EgressActive as i32,
                ended_at: 0,
                ..Default::default()
            })
            .expect("egress should store");

        let response = post(
            router(state.clone()),
            "/twirp/livekit.Egress/StopEgress",
            Some(token(VideoGrants {
                room_record: true,
                ..Default::default()
            })),
            proto::StopEgressRequest {
                egress_id: "EG_900".to_string(),
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::EgressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert_eq!(payload.egress_id, "EG_900");
        assert_eq!(payload.status, proto::EgressStatus::EgressComplete as i32);
        assert!(payload.ended_at > 0);

        let stored = state
            .rooms
            .load_egress_info("EG_900")
            .expect("egress should remain stored");
        assert_eq!(stored.status, proto::EgressStatus::EgressComplete as i32);
        assert!(stored.ended_at > 0);
    }

    #[tokio::test]
    async fn update_layout_updates_room_composite_request_layout() {
        let state = test_state();
        state
            .rooms
            .store_egress_info(&proto::EgressInfo {
                egress_id: "EG_LAYOUT".to_string(),
                room_name: "room-a".to_string(),
                status: proto::EgressStatus::EgressActive as i32,
                request: Some(proto::egress_info::Request::RoomComposite(
                    proto::RoomCompositeEgressRequest {
                        room_name: "room-a".to_string(),
                        layout: "speaker-dark".to_string(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            })
            .expect("egress should store");

        let response = post(
            router(state.clone()),
            "/twirp/livekit.Egress/UpdateLayout",
            Some(token(VideoGrants {
                room_record: true,
                ..Default::default()
            })),
            proto::UpdateLayoutRequest {
                egress_id: "EG_LAYOUT".to_string(),
                layout: "grid-light".to_string(),
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::EgressInfo::decode(body_bytes(response).await).expect("payload decode");
        let request = payload.request.expect("egress request should exist");
        let proto::egress_info::Request::RoomComposite(room_req) = request else {
            panic!("expected room composite request");
        };
        assert_eq!(room_req.layout, "grid-light");
    }

    #[tokio::test]
    async fn update_stream_adds_and_removes_urls_from_stream_results() {
        let state = test_state();
        state
            .rooms
            .store_egress_info(&proto::EgressInfo {
                egress_id: "EG_STREAM".to_string(),
                room_name: "room-a".to_string(),
                status: proto::EgressStatus::EgressActive as i32,
                stream_results: vec![proto::StreamInfo {
                    url: "rtmp://old.example/live/stream".to_string(),
                    status: proto::stream_info::Status::Active as i32,
                    ..Default::default()
                }],
                ..Default::default()
            })
            .expect("egress should store");

        let response = post(
            router(state.clone()),
            "/twirp/livekit.Egress/UpdateStream",
            Some(token(VideoGrants {
                room_record: true,
                ..Default::default()
            })),
            proto::UpdateStreamRequest {
                egress_id: "EG_STREAM".to_string(),
                add_output_urls: vec!["rtmp://new.example/live/stream".to_string()],
                remove_output_urls: vec!["rtmp://old.example/live/stream".to_string()],
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::EgressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert_eq!(payload.stream_results.len(), 1);
        assert_eq!(
            payload.stream_results[0].url,
            "rtmp://new.example/live/stream"
        );
    }

    #[tokio::test]
    async fn update_stream_returns_failed_precondition_for_completed_egress() {
        let state = test_state();
        state
            .rooms
            .store_egress_info(&proto::EgressInfo {
                egress_id: "EG_DONE".to_string(),
                status: proto::EgressStatus::EgressComplete as i32,
                ..Default::default()
            })
            .expect("egress should store");

        let response = post(
            router(state),
            "/twirp/livekit.Egress/UpdateStream",
            Some(token(VideoGrants {
                room_record: true,
                ..Default::default()
            })),
            proto::UpdateStreamRequest {
                egress_id: "EG_DONE".to_string(),
                add_output_urls: vec!["rtmp://new.example/live/stream".to_string()],
                ..Default::default()
            },
        )
        .await;

        assert_twirp_error(
            response,
            StatusCode::PRECONDITION_FAILED,
            "failed_precondition",
        )
        .await;
    }

    #[tokio::test]
    async fn start_egress_creates_and_returns_new_egress_info() {
        let state = test_state();

        let response = post(
            router(state.clone()),
            "/twirp/livekit.Egress/StartEgress",
            Some(token(VideoGrants {
                room_record: true,
                ..Default::default()
            })),
            proto::StartEgressRequest {
                room_name: "room-a".to_string(),
                outputs: vec![proto::Output {
                    config: Some(proto::output::Config::Stream(proto::StreamOutput {
                        protocol: proto::StreamProtocol::Rtmp as i32,
                        urls: vec!["rtmp://example.com/live/a".to_string()],
                    })),
                    ..Default::default()
                }],
                source: Some(proto::start_egress_request::Source::Template(
                    proto::TemplateSource {
                        layout: "speaker-dark".to_string(),
                        ..Default::default()
                    },
                )),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let payload =
            proto::EgressInfo::decode(body_bytes(response).await).expect("payload decode");
        assert!(payload.egress_id.starts_with("EG_"));
        assert_eq!(payload.room_name, "room-a");
        assert_eq!(payload.status, proto::EgressStatus::EgressStarting as i32);

        let stored = state
            .rooms
            .load_egress_info(&payload.egress_id)
            .expect("egress should persist");
        assert_eq!(stored.room_name, "room-a");
    }

    #[tokio::test]
    async fn unsupported_ingress_egress_paths_reject_non_post_methods() {
        for path in ["/twirp/livekit.Egress/StartEgress"] {
            let response = get(path).await;
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        }
    }

    #[tokio::test]
    async fn create_room_accepts_livekit_twirp_protobuf() {
        let app = router(test_state());
        let response = post(
            app,
            "/twirp/livekit.RoomService/CreateRoom",
            Some(token(VideoGrants {
                room_create: true,
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                metadata: "hello".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let room = proto::Room::decode(body_bytes(response).await).expect("room should decode");
        assert_eq!(room.name, "test-room");
        assert_eq!(room.metadata, "hello");
    }

    #[tokio::test]
    async fn create_room_accepts_livekit_twirp_json() {
        let bearer = format!(
            "Bearer {}",
            token(VideoGrants {
                room_create: true,
                room_admin: true,
                room: "test-room-json".to_string(),
                ..Default::default()
            })
        );
        let response = post_json_with_authorization_header(
            router(test_state()),
            "/twirp/livekit.RoomService/CreateRoom",
            Some(&bearer),
            &proto::CreateRoomRequest {
                name: "test-room-json".to_string(),
                metadata: "hello-json".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&header::HeaderValue::from_static(APPLICATION_JSON)),
        );
        let room: proto::Room =
            serde_json::from_slice(&body_bytes(response).await).expect("json room should decode");
        assert_eq!(room.name, "test-room-json");
        assert_eq!(room.metadata, "hello-json");
    }

    #[tokio::test]
    async fn create_room_rejects_metadata_over_512_kib() {
        let app = router(test_state());
        let admin = token(VideoGrants {
            room_create: true,
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let response = post(
            app,
            "/twirp/livekit.RoomService/CreateRoom",
            Some(admin),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                metadata: "m".repeat(512 * 1024 + 1),
                ..Default::default()
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::BAD_REQUEST, "invalid_argument").await;
    }

    #[tokio::test]
    async fn create_room_accepts_metadata_at_512_kib_limit() {
        let app = router(test_state());
        let admin = token(VideoGrants {
            room_create: true,
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let response = post(
            app,
            "/twirp/livekit.RoomService/CreateRoom",
            Some(admin),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                metadata: "m".repeat(512 * 1024),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let room = proto::Room::decode(body_bytes(response).await).expect("room should decode");
        assert_eq!(room.metadata.len(), 512 * 1024);
    }

    #[tokio::test]
    async fn list_rooms_returns_created_room() {
        let state = test_state();
        let app = router(state);
        let create_token = token(VideoGrants {
            room_create: true,
            ..Default::default()
        });
        let list_token = token(VideoGrants {
            room_list: true,
            ..Default::default()
        });

        let create_response = post(
            app.clone(),
            "/twirp/livekit.RoomService/CreateRoom",
            Some(create_token),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(create_response.status(), StatusCode::OK);

        let list_response = post(
            app,
            "/twirp/livekit.RoomService/ListRooms",
            Some(list_token),
            proto::ListRoomsRequest::default(),
        )
        .await;

        assert_eq!(list_response.status(), StatusCode::OK);
        let rooms = proto::ListRoomsResponse::decode(body_bytes(list_response).await)
            .expect("rooms should decode");
        assert_eq!(rooms.rooms.len(), 1);
        assert_eq!(rooms.rooms[0].name, "test-room");
    }

    #[tokio::test]
    async fn get_participant_returns_not_found_until_signalling_adds_participants() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/GetParticipant",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "not_found");
        assert_eq!(body["msg"], "participant not found");
    }

    #[tokio::test]
    async fn remove_participant_removes_joined_participant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let rooms = state.rooms.clone();
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/RemoveParticipant",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        proto::RemoveParticipantResponse::decode(body_bytes(response).await)
            .expect("remove participant response should decode");
        assert_eq!(
            rooms.get_participant("test-room", "alice"),
            Err(RoomStoreError::ParticipantNotFound)
        );
    }

    #[tokio::test]
    async fn remove_participant_invokes_runtime_disconnect_with_participant_removed_reason() {
        let runtime = Arc::new(RecordingRuntime::default());
        let state = test_state_with_runtime(Some(runtime.clone()));
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/RemoveParticipant",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            runtime
                .calls
                .lock()
                .expect("runtime calls should lock")
                .as_slice(),
            &[RuntimeCall::DisconnectParticipant {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                reason: proto::DisconnectReason::ParticipantRemoved,
            }]
        );
    }

    #[tokio::test]
    async fn delete_room_invokes_runtime_disconnect_with_room_deleted_reason() {
        let runtime = Arc::new(RecordingRuntime::default());
        let state = test_state_with_runtime(Some(runtime.clone()));
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/DeleteRoom",
            Some(token(VideoGrants {
                room_create: true,
                ..Default::default()
            })),
            proto::DeleteRoomRequest {
                room: "test-room".to_string(),
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            runtime
                .calls
                .lock()
                .expect("runtime calls should lock")
                .as_slice(),
            &[
                RuntimeCall::DisconnectRoomParticipants {
                    room: "test-room".to_string(),
                    reason: proto::DisconnectReason::RoomDeleted,
                },
                RuntimeCall::RoomDeleted {
                    room: "test-room".to_string(),
                }
            ]
        );
    }

    #[tokio::test]
    async fn update_participant_invokes_runtime_participant_update_broadcast() {
        let runtime = Arc::new(RecordingRuntime::default());
        let state = test_state_with_runtime(Some(runtime.clone()));
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/UpdateParticipant",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateParticipantRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                metadata: "updated".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            runtime
                .calls
                .lock()
                .expect("runtime calls should lock")
                .as_slice(),
            &[RuntimeCall::BroadcastParticipantUpdate {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn mute_published_track_invokes_runtime_participant_update_broadcast() {
        let runtime = Arc::new(RecordingRuntime::default());
        let state = test_state_with_runtime(Some(runtime.clone()));
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "alice",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    ..Default::default()
                },
            )
            .expect("track should add");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/MutePublishedTrack",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::MuteRoomTrackRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                track_sid: "TR_test".to_string(),
                muted: true,
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            runtime
                .calls
                .lock()
                .expect("runtime calls should lock")
                .as_slice(),
            &[RuntimeCall::BroadcastParticipantUpdate {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
            }]
        );
    }

    #[tokio::test]
    async fn move_participant_requires_destination_room_grant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "source",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "dest".to_string(),
                ..Default::default()
            })
            .expect("destination room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/MoveParticipant",
            Some(token(VideoGrants {
                room_admin: true,
                room: "source".to_string(),
                ..Default::default()
            })),
            proto::MoveParticipantRequest {
                room: "source".to_string(),
                identity: "alice".to_string(),
                destination_room: "dest".to_string(),
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
    }

    #[tokio::test]
    async fn forward_participant_rejects_same_source_and_destination_room() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "source",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/ForwardParticipant",
            Some(token(VideoGrants {
                room_admin: true,
                room: "source".to_string(),
                destination_room: "source".to_string(),
                ..Default::default()
            })),
            proto::ForwardParticipantRequest {
                room: "source".to_string(),
                identity: "alice".to_string(),
                destination_room: "source".to_string(),
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "invalid_argument");
    }

    #[tokio::test]
    async fn update_subscriptions_by_track_sid_toggles_room_store_media_preference() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "publisher",
                "Publisher",
                String::new(),
                Default::default(),
            )
            .expect("publisher should join");
        state
            .rooms
            .join_participant(
                "test-room",
                "subscriber",
                "Subscriber",
                String::new(),
                Default::default(),
            )
            .expect("subscriber should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    muted: false,
                    ..Default::default()
                },
            )
            .expect("publisher track should be added");

        let rooms = state.rooms.clone();
        let app = router(state);

        let unsubscribe_response = post(
            app.clone(),
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "subscriber".to_string(),
                track_sids: vec!["TR_test".to_string()],
                subscribe: false,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(unsubscribe_response.status(), StatusCode::OK);
        proto::UpdateSubscriptionsResponse::decode(body_bytes(unsubscribe_response).await)
            .expect("unsubscribe response should decode");
        assert!(
            !rooms.is_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber"),
            "track SID-based unsubscribe should persist in room store"
        );

        let subscribe_response = post(
            app,
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "subscriber".to_string(),
                track_sids: vec!["TR_test".to_string()],
                subscribe: true,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(subscribe_response.status(), StatusCode::OK);
        proto::UpdateSubscriptionsResponse::decode(body_bytes(subscribe_response).await)
            .expect("subscribe response should decode");
        assert!(
            rooms.is_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber"),
            "track SID-based subscribe should persist in room store"
        );
    }

    #[tokio::test]
    async fn update_subscriptions_by_participant_sid_toggles_room_store_media_preference() {
        let state = test_state();
        let (_, publisher, _) = state
            .rooms
            .join_participant(
                "test-room",
                "publisher",
                "Publisher",
                String::new(),
                Default::default(),
            )
            .expect("publisher should join");
        state
            .rooms
            .join_participant(
                "test-room",
                "subscriber",
                "Subscriber",
                String::new(),
                Default::default(),
            )
            .expect("subscriber should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    muted: false,
                    ..Default::default()
                },
            )
            .expect("publisher track should be added");

        let rooms = state.rooms.clone();
        let app = router(state);

        let unsubscribe_response = post(
            app.clone(),
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "subscriber".to_string(),
                subscribe: false,
                participant_tracks: vec![proto::ParticipantTracks {
                    participant_sid: publisher.sid.clone(),
                    track_sids: vec!["TR_test".to_string()],
                }],
                ..Default::default()
            },
        )
        .await;

        assert_eq!(unsubscribe_response.status(), StatusCode::OK);
        proto::UpdateSubscriptionsResponse::decode(body_bytes(unsubscribe_response).await)
            .expect("unsubscribe response should decode");
        assert!(
            !rooms.is_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber"),
            "participant SID-based unsubscribe should persist in room store"
        );

        let subscribe_response = post(
            app,
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "subscriber".to_string(),
                subscribe: true,
                participant_tracks: vec![proto::ParticipantTracks {
                    participant_sid: publisher.sid,
                    track_sids: vec!["TR_test".to_string()],
                }],
                ..Default::default()
            },
        )
        .await;

        assert_eq!(subscribe_response.status(), StatusCode::OK);
        proto::UpdateSubscriptionsResponse::decode(body_bytes(subscribe_response).await)
            .expect("subscribe response should decode");
        assert!(
            rooms.is_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber"),
            "participant SID-based subscribe should persist in room store"
        );
    }

    #[tokio::test]
    async fn update_subscriptions_returns_not_found_for_missing_participant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "publisher",
                "Publisher",
                String::new(),
                Default::default(),
            )
            .expect("publisher should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    muted: false,
                    ..Default::default()
                },
            )
            .expect("publisher track should be added");

        let app = router(state);
        let response = post(
            app,
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "missing-subscriber".to_string(),
                track_sids: vec!["TR_test".to_string()],
                subscribe: false,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "not_found");
        assert_eq!(body["msg"], "participant not found");
    }

    #[tokio::test]
    async fn update_subscriptions_returns_permission_denied_without_room_admin() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "publisher",
                "Publisher",
                String::new(),
                Default::default(),
            )
            .expect("publisher should join");
        state
            .rooms
            .join_participant(
                "test-room",
                "subscriber",
                "Subscriber",
                String::new(),
                Default::default(),
            )
            .expect("subscriber should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    muted: false,
                    ..Default::default()
                },
            )
            .expect("publisher track should be added");

        let app = router(state);
        let response = post(
            app,
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(token(VideoGrants {
                room_admin: false,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "subscriber".to_string(),
                track_sids: vec!["TR_test".to_string()],
                subscribe: false,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "permission_denied");
        assert_eq!(body["msg"], "permissions denied");
    }

    #[tokio::test]
    async fn send_data_succeeds_for_existing_room() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"hello".to_vec(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        proto::SendDataResponse::decode(body_bytes(response).await)
            .expect("send data response should decode");
    }

    #[tokio::test]
    async fn send_data_rejects_nonce_when_not_16_bytes() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"hello".to_vec(),
                nonce: vec![0xAA; 15],
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::BAD_REQUEST);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "invalid_argument");
        assert_eq!(
            body["msg"],
            "nonce should be 16-bytes or not present, got: 15 bytes"
        );
    }

    #[tokio::test]
    async fn send_data_accepts_16_byte_nonce() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"hello".to_vec(),
                nonce: vec![0xBB; 16],
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        proto::SendDataResponse::decode(body_bytes(response).await)
            .expect("send data response should decode for 16-byte nonce");
    }

    #[tokio::test]
    async fn send_data_returns_not_found_for_missing_room() {
        let app = router(test_state());

        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: true,
                room: "missing-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "missing-room".to_string(),
                data: b"hello".to_vec(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::NOT_FOUND);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "not_found");
        assert_eq!(body["msg"], "room not found");
    }

    #[tokio::test]
    async fn send_data_returns_permission_denied_without_room_admin() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: false,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"hello".to_vec(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::FORBIDDEN);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "permission_denied");
        assert_eq!(body["msg"], "permissions denied");
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn send_data_destination_filter_sends_only_named_participants_and_preserves_topic() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");

        let (alice_offer_pc, alice_answer_pc, alice_server_channel, alice_client_channel) =
            connected_data_channel_pair().await;
        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;

        state.data_channels.insert_with_kind(
            "test-room",
            "alice",
            oxidesfu_rtc::DataChannelKind::Reliable,
            alice_server_channel,
        );
        state.data_channels.insert_with_kind(
            "test-room",
            "bob",
            oxidesfu_rtc::DataChannelKind::Reliable,
            bob_server_channel,
        );

        let app = router(state);
        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"targeted-data".to_vec(),
                destination_identities: vec!["bob".to_string()],
                topic: Some("admin-topic".to_string()),
                kind: proto::data_packet::Kind::Reliable as i32,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);

        let bob_packet = recv_packet_with_timeout(&bob_client_channel, Duration::from_secs(3))
            .await
            .expect("bob should receive targeted packet");
        assert_eq!(bob_packet.kind, proto::data_packet::Kind::Reliable as i32);
        let Some(proto::data_packet::Value::User(user)) = bob_packet.value else {
            panic!("expected user packet");
        };
        assert_eq!(user.payload, b"targeted-data");
        assert_eq!(user.topic.as_deref(), Some("admin-topic"));

        let alice_packet =
            recv_packet_with_timeout(&alice_client_channel, Duration::from_millis(400)).await;
        assert!(
            alice_packet.is_none(),
            "non-destination participant should not receive targeted packet"
        );

        alice_offer_pc
            .close()
            .await
            .expect("alice offer pc should close");
        alice_answer_pc
            .close()
            .await
            .expect("alice answer pc should close");
        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn send_data_empty_destinations_broadcasts_to_room() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");

        let (alice_offer_pc, alice_answer_pc, alice_server_channel, alice_client_channel) =
            connected_data_channel_pair().await;
        let (bob_offer_pc, bob_answer_pc, bob_server_channel, bob_client_channel) =
            connected_data_channel_pair().await;

        state.data_channels.insert_with_kind(
            "test-room",
            "alice",
            oxidesfu_rtc::DataChannelKind::Reliable,
            alice_server_channel,
        );
        state.data_channels.insert_with_kind(
            "test-room",
            "bob",
            oxidesfu_rtc::DataChannelKind::Reliable,
            bob_server_channel,
        );

        let app = router(state);
        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"broadcast-data".to_vec(),
                topic: Some("broadcast-topic".to_string()),
                kind: proto::data_packet::Kind::Reliable as i32,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);

        let alice_packet = recv_packet_with_timeout(&alice_client_channel, Duration::from_secs(3))
            .await
            .expect("alice should receive broadcast packet");
        let bob_packet = recv_packet_with_timeout(&bob_client_channel, Duration::from_secs(3))
            .await
            .expect("bob should receive broadcast packet");

        for packet in [alice_packet, bob_packet] {
            assert_eq!(packet.kind, proto::data_packet::Kind::Reliable as i32);
            let Some(proto::data_packet::Value::User(user)) = packet.value else {
                panic!("expected user packet");
            };
            assert_eq!(user.payload, b"broadcast-data");
            assert_eq!(user.topic.as_deref(), Some("broadcast-topic"));
        }

        alice_offer_pc
            .close()
            .await
            .expect("alice offer pc should close");
        alice_answer_pc
            .close()
            .await
            .expect("alice answer pc should close");
        bob_offer_pc
            .close()
            .await
            .expect("bob offer pc should close");
        bob_answer_pc
            .close()
            .await
            .expect("bob answer pc should close");
    }

    #[tokio::test]
    #[allow(deprecated)]
    async fn send_data_lossy_kind_routes_over_lossy_channel_kind() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");

        let (
            reliable_offer_pc,
            reliable_answer_pc,
            reliable_server_channel,
            reliable_client_channel,
        ) = connected_data_channel_pair().await;
        let (lossy_offer_pc, lossy_answer_pc, lossy_server_channel, lossy_client_channel) =
            connected_data_channel_pair().await;

        state.data_channels.insert_with_kind(
            "test-room",
            "alice",
            oxidesfu_rtc::DataChannelKind::Reliable,
            reliable_server_channel,
        );
        state.data_channels.insert_with_kind(
            "test-room",
            "alice",
            oxidesfu_rtc::DataChannelKind::Lossy,
            lossy_server_channel,
        );

        let app = router(state);
        let response = post(
            app,
            "/twirp/livekit.RoomService/SendData",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"lossy-data".to_vec(),
                destination_identities: vec!["alice".to_string()],
                kind: proto::data_packet::Kind::Lossy as i32,
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);

        let lossy_packet = recv_packet_with_timeout(&lossy_client_channel, Duration::from_secs(3))
            .await
            .expect("lossy channel should receive lossy packet");
        assert_eq!(lossy_packet.kind, proto::data_packet::Kind::Lossy as i32);
        let Some(proto::data_packet::Value::User(user)) = lossy_packet.value else {
            panic!("expected user packet");
        };
        assert_eq!(user.payload, b"lossy-data");

        let reliable_packet =
            recv_packet_with_timeout(&reliable_client_channel, Duration::from_millis(400)).await;
        assert!(
            reliable_packet.is_none(),
            "lossy send should not be routed over reliable channel kind"
        );

        reliable_offer_pc
            .close()
            .await
            .expect("reliable offer pc should close");
        reliable_answer_pc
            .close()
            .await
            .expect("reliable answer pc should close");
        lossy_offer_pc
            .close()
            .await
            .expect("lossy offer pc should close");
        lossy_answer_pc
            .close()
            .await
            .expect("lossy answer pc should close");
    }

    #[tokio::test]
    async fn create_room_without_auth_returns_twirp_unauthenticated_error() {
        let app = router(test_state());
        let response = post(
            app,
            "/twirp/livekit.RoomService/CreateRoom",
            None,
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "unauthenticated");
    }

    #[tokio::test]
    async fn create_room_rejects_non_bearer_authorization_header() {
        let app = router(test_state());
        let response = post_with_authorization_header(
            app,
            "/twirp/livekit.RoomService/CreateRoom",
            Some("Token abc"),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "unauthenticated");
    }

    #[tokio::test]
    async fn create_room_rejects_malformed_bearer_jwt() {
        let app = router(test_state());
        let response = post_with_authorization_header(
            app,
            "/twirp/livekit.RoomService/CreateRoom",
            Some("Bearer not-a-jwt"),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::UNAUTHORIZED);
        let body: serde_json::Value =
            serde_json::from_slice(&body_bytes(response).await).expect("error should be JSON");
        assert_eq!(body["code"], "unauthenticated");
    }

    #[tokio::test]
    async fn optional_auth_middleware_passes_through_missing_auth_and_attaches_valid_auth_context()
    {
        async fn probe_auth(maybe_auth: Option<axum::extract::Extension<AuthContext>>) -> Response {
            let authenticated = maybe_auth.is_some();
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from(if authenticated {
                    "authenticated"
                } else {
                    "anonymous"
                }))
                .expect("probe response should build")
        }

        let state = test_state();
        let app = Router::new()
            .route("/auth-probe", axum::routing::post(probe_auth))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                optional_auth_middleware,
            ))
            .with_state(state);

        let anonymous_request = Request::builder()
            .method("POST")
            .uri("/auth-probe")
            .body(Body::empty())
            .expect("anonymous request should build");
        let anonymous_response = app
            .clone()
            .oneshot(anonymous_request)
            .await
            .expect("anonymous probe should respond");
        assert_eq!(anonymous_response.status(), StatusCode::OK);
        assert_eq!(
            body_bytes(anonymous_response).await,
            Bytes::from_static(b"anonymous")
        );

        let valid_bearer = format!(
            "Bearer {}",
            token(VideoGrants {
                room: "test-room".to_string(),
                room_join: true,
                ..Default::default()
            })
        );
        let authenticated_request = Request::builder()
            .method("POST")
            .uri("/auth-probe")
            .header(header::AUTHORIZATION, valid_bearer)
            .body(Body::empty())
            .expect("authenticated request should build");
        let authenticated_response = app
            .clone()
            .oneshot(authenticated_request)
            .await
            .expect("authenticated probe should respond");
        assert_eq!(authenticated_response.status(), StatusCode::OK);
        assert_eq!(
            body_bytes(authenticated_response).await,
            Bytes::from_static(b"authenticated")
        );
    }

    #[tokio::test]
    async fn optional_auth_middleware_rejects_invalid_authorization_header() {
        async fn probe_auth(
            _maybe_auth: Option<axum::extract::Extension<AuthContext>>,
        ) -> Response {
            Response::builder()
                .status(StatusCode::OK)
                .body(Body::from("should-not-reach"))
                .expect("probe response should build")
        }

        let state = test_state();
        let app = Router::new()
            .route("/auth-probe", axum::routing::post(probe_auth))
            .layer(axum::middleware::from_fn_with_state(
                state.clone(),
                optional_auth_middleware,
            ))
            .with_state(state);

        let invalid_request = Request::builder()
            .method("POST")
            .uri("/auth-probe")
            .header(header::AUTHORIZATION, "Bearer not-a-jwt")
            .body(Body::empty())
            .expect("invalid request should build");
        let invalid_response = app
            .oneshot(invalid_request)
            .await
            .expect("invalid probe should respond");
        assert_eq!(invalid_response.status(), StatusCode::UNAUTHORIZED);
    }

    #[tokio::test]
    async fn list_participants_requires_room_admin_grant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/ListParticipants",
            Some(token(VideoGrants {
                room_list: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::ListParticipantsRequest {
                room: "test-room".to_string(),
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn create_room_requires_room_create_grant() {
        let app = router(test_state());
        let response = post(
            app,
            "/twirp/livekit.RoomService/CreateRoom",
            Some(token(VideoGrants {
                room_create: false,
                ..Default::default()
            })),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn delete_room_requires_room_create_grant() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/DeleteRoom",
            Some(token(VideoGrants::default())),
            proto::DeleteRoomRequest {
                room: "test-room".to_string(),
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn list_rooms_requires_room_list_grant() {
        let app = router(test_state());
        let response = post(
            app,
            "/twirp/livekit.RoomService/ListRooms",
            Some(token(VideoGrants::default())),
            proto::ListRoomsRequest::default(),
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn get_participant_requires_room_admin_grant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/GetParticipant",
            Some(token(VideoGrants {
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn remove_participant_requires_room_admin_grant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/RemoveParticipant",
            Some(token(VideoGrants {
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn update_participant_requires_room_admin_grant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/UpdateParticipant",
            Some(token(VideoGrants {
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateParticipantRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                metadata: "updated".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn mute_published_track_requires_room_admin_grant() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "alice",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    ..Default::default()
                },
            )
            .expect("track should add");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/MutePublishedTrack",
            Some(token(VideoGrants {
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::MuteRoomTrackRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                track_sid: "TR_test".to_string(),
                muted: true,
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn move_participant_requires_room_admin_and_destination_room_grants() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "source",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "dest".to_string(),
                ..Default::default()
            })
            .expect("destination room should create");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/MoveParticipant",
            Some(token(VideoGrants {
                room_admin: false,
                room: "source".to_string(),
                destination_room: "dest".to_string(),
                ..Default::default()
            })),
            proto::MoveParticipantRequest {
                room: "source".to_string(),
                identity: "alice".to_string(),
                destination_room: "dest".to_string(),
            },
        )
        .await;

        assert_twirp_error(response, StatusCode::FORBIDDEN, "permission_denied").await;
    }

    #[tokio::test]
    async fn forward_participant_requires_room_admin_and_destination_room_grants() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "source",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "dest".to_string(),
                ..Default::default()
            })
            .expect("destination room should create");
        let app = router(state);

        let missing_admin = post(
            app.clone(),
            "/twirp/livekit.RoomService/ForwardParticipant",
            Some(token(VideoGrants {
                room_admin: false,
                room: "source".to_string(),
                destination_room: "dest".to_string(),
                ..Default::default()
            })),
            proto::ForwardParticipantRequest {
                room: "source".to_string(),
                identity: "alice".to_string(),
                destination_room: "dest".to_string(),
            },
        )
        .await;
        assert_twirp_error(missing_admin, StatusCode::FORBIDDEN, "permission_denied").await;

        let missing_destination = post(
            app,
            "/twirp/livekit.RoomService/ForwardParticipant",
            Some(token(VideoGrants {
                room_admin: true,
                room: "source".to_string(),
                destination_room: String::new(),
                ..Default::default()
            })),
            proto::ForwardParticipantRequest {
                room: "source".to_string(),
                identity: "alice".to_string(),
                destination_room: "dest".to_string(),
            },
        )
        .await;
        assert_twirp_error(
            missing_destination,
            StatusCode::FORBIDDEN,
            "permission_denied",
        )
        .await;
    }

    #[tokio::test]
    async fn twirp_auth_errors_have_livekit_compatible_error_json() {
        let app = router(test_state());

        let missing = post_with_authorization_header(
            app.clone(),
            "/twirp/livekit.RoomService/ListRooms",
            None,
            proto::ListRoomsRequest::default(),
        )
        .await;
        assert_twirp_error(missing, StatusCode::UNAUTHORIZED, "unauthenticated").await;

        let non_bearer = post_with_authorization_header(
            app.clone(),
            "/twirp/livekit.RoomService/ListRooms",
            Some("Token abc"),
            proto::ListRoomsRequest::default(),
        )
        .await;
        assert_twirp_error(non_bearer, StatusCode::UNAUTHORIZED, "unauthenticated").await;

        let malformed = post_with_authorization_header(
            app,
            "/twirp/livekit.RoomService/ListRooms",
            Some("Bearer not-a-jwt"),
            proto::ListRoomsRequest::default(),
        )
        .await;
        assert_twirp_error(malformed, StatusCode::UNAUTHORIZED, "unauthenticated").await;
    }

    #[tokio::test]
    async fn room_service_rejects_unknown_method_path_with_not_found() {
        let response = post(
            router(test_state()),
            "/twirp/livekit.RoomService/NoSuchMethod",
            Some(token(VideoGrants {
                room_list: true,
                ..Default::default()
            })),
            proto::ListRoomsRequest::default(),
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn room_service_rejects_wrong_service_path() {
        let response = post(
            router(test_state()),
            "/twirp/livekit.NotRoomService/CreateRoom",
            Some(token(VideoGrants {
                room_create: true,
                ..Default::default()
            })),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::NOT_FOUND);
    }

    #[tokio::test]
    async fn room_service_json_body_policy_is_stable_accepts_json() {
        let app = router(test_state());
        let response = app
            .clone()
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/twirp/livekit.RoomService/CreateRoom")
                    .header(
                        header::AUTHORIZATION,
                        format!(
                            "Bearer {}",
                            token(VideoGrants {
                                room_create: true,
                                ..Default::default()
                            })
                        ),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{\"name\":\"json-room\"}"))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&header::HeaderValue::from_static(APPLICATION_JSON)),
        );
        let room: proto::Room =
            serde_json::from_slice(&body_bytes(response).await).expect("json room should decode");
        assert_eq!(room.name, "json-room");

        let response = app
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/twirp/livekit.RoomService/ListRooms")
                    .header(
                        header::AUTHORIZATION,
                        format!(
                            "Bearer {}",
                            token(VideoGrants {
                                room_list: true,
                                ..Default::default()
                            })
                        ),
                    )
                    .header(header::CONTENT_TYPE, "application/json")
                    .body(Body::from("{}"))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");
        assert_eq!(response.status(), StatusCode::OK);
        assert_eq!(
            response.headers().get(header::CONTENT_TYPE),
            Some(&header::HeaderValue::from_static(APPLICATION_JSON)),
        );
        let list_response: proto::ListRoomsResponse =
            serde_json::from_slice(&body_bytes(response).await)
                .expect("json list rooms response should decode");
        assert!(
            list_response
                .rooms
                .iter()
                .any(|room| room.name == "json-room"),
            "list rooms response should include room created via JSON Twirp"
        );
    }

    #[tokio::test]
    async fn room_service_rejects_malformed_protobuf_with_twirp_malformed_error() {
        let response = router(test_state())
            .oneshot(
                Request::builder()
                    .method("POST")
                    .uri("/twirp/livekit.RoomService/ListRooms")
                    .header(
                        header::AUTHORIZATION,
                        format!(
                            "Bearer {}",
                            token(VideoGrants {
                                room_list: true,
                                ..Default::default()
                            })
                        ),
                    )
                    .header(header::CONTENT_TYPE, APPLICATION_PROTOBUF)
                    .body(Body::from(vec![0xFF, 0x00, 0xAB]))
                    .expect("request should build"),
            )
            .await
            .expect("router should respond");

        assert_twirp_error(response, StatusCode::BAD_REQUEST, "malformed").await;
    }

    #[tokio::test]
    async fn room_service_rejects_non_post_methods() {
        let app = router(test_state());
        for method in [
            axum::http::Method::GET,
            axum::http::Method::PUT,
            axum::http::Method::DELETE,
        ] {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method(method)
                        .uri("/twirp/livekit.RoomService/ListRooms")
                        .header(
                            header::AUTHORIZATION,
                            format!(
                                "Bearer {}",
                                token(VideoGrants {
                                    room_list: true,
                                    ..Default::default()
                                })
                            ),
                        )
                        .body(Body::empty())
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            assert_eq!(response.status(), StatusCode::METHOD_NOT_ALLOWED);
        }
    }

    #[tokio::test]
    async fn create_room_requires_non_empty_name() {
        let response = post(
            router(test_state()),
            "/twirp/livekit.RoomService/CreateRoom",
            Some(token(VideoGrants {
                room_create: true,
                ..Default::default()
            })),
            proto::CreateRoomRequest::default(),
        )
        .await;
        assert_twirp_error(response, StatusCode::BAD_REQUEST, "invalid_argument").await;
    }

    #[tokio::test]
    async fn create_room_defaults_timeouts_to_livekit_values() {
        let response = post(
            router(test_state()),
            "/twirp/livekit.RoomService/CreateRoom",
            Some(token(VideoGrants {
                room_create: true,
                ..Default::default()
            })),
            proto::CreateRoomRequest {
                name: "defaults-room".to_string(),
                ..Default::default()
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let room = proto::Room::decode(body_bytes(response).await).expect("room should decode");
        assert_eq!(room.empty_timeout, 300);
        assert_eq!(room.departure_timeout, 20);
    }

    #[tokio::test]
    async fn list_rooms_num_participants_excludes_hidden_participants() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "visible",
                "Visible",
                String::new(),
                Default::default(),
            )
            .expect("visible should join");
        state
            .rooms
            .join_participant_with_permission(
                "test-room",
                "hidden",
                "Hidden",
                String::new(),
                Default::default(),
                Some(proto::ParticipantPermission {
                    hidden: true,
                    ..Default::default()
                }),
            )
            .expect("hidden should join");
        for identity in ["visible", "hidden"] {
            state
                .rooms
                .add_participant_track(
                    "test-room",
                    identity,
                    proto::TrackInfo {
                        sid: format!("TR_{identity}"),
                        ..Default::default()
                    },
                )
                .expect("publisher track should be stored");
        }

        let response = post(
            router(state),
            "/twirp/livekit.RoomService/ListRooms",
            Some(token(VideoGrants {
                room_list: true,
                ..Default::default()
            })),
            proto::ListRoomsRequest::default(),
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let rooms =
            proto::ListRoomsResponse::decode(body_bytes(response).await).expect("rooms decode");
        assert_eq!(rooms.rooms.len(), 1);
        assert_eq!(rooms.rooms[0].num_participants, 1);
        // LiveKit counts publishers independently from hidden-participant visibility.
        assert_eq!(rooms.rooms[0].num_publishers, 2);
    }

    #[tokio::test]
    async fn list_rooms_filters_by_names_and_excludes_deleted() {
        let app = router(test_state());
        let create_token = token(VideoGrants {
            room_create: true,
            ..Default::default()
        });
        let list_token = token(VideoGrants {
            room_list: true,
            ..Default::default()
        });

        for room in ["room-a", "room-b", "room-c"] {
            let response = post(
                app.clone(),
                "/twirp/livekit.RoomService/CreateRoom",
                Some(create_token.clone()),
                proto::CreateRoomRequest {
                    name: room.to_string(),
                    ..Default::default()
                },
            )
            .await;
            assert_eq!(response.status(), StatusCode::OK);
        }

        let delete_response = post(
            app.clone(),
            "/twirp/livekit.RoomService/DeleteRoom",
            Some(create_token),
            proto::DeleteRoomRequest {
                room: "room-b".to_string(),
            },
        )
        .await;
        assert_eq!(delete_response.status(), StatusCode::OK);

        let response = post(
            app.clone(),
            "/twirp/livekit.RoomService/ListRooms",
            Some(list_token.clone()),
            proto::ListRoomsRequest {
                names: vec!["room-a".to_string(), "room-c".to_string()],
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let filtered =
            proto::ListRoomsResponse::decode(body_bytes(response).await).expect("rooms decode");
        assert_eq!(filtered.rooms.len(), 2);
        assert_eq!(filtered.rooms[0].name, "room-a");
        assert_eq!(filtered.rooms[1].name, "room-c");

        let response = post(
            app,
            "/twirp/livekit.RoomService/ListRooms",
            Some(list_token),
            proto::ListRoomsRequest {
                names: vec!["room-b".to_string()],
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let filtered =
            proto::ListRoomsResponse::decode(body_bytes(response).await).expect("rooms decode");
        assert!(filtered.rooms.is_empty());
    }

    #[tokio::test]
    async fn delete_room_returns_not_found_for_missing_room_and_second_delete() {
        let app = router(test_state());
        let token = token(VideoGrants {
            room_create: true,
            ..Default::default()
        });

        let missing = post(
            app.clone(),
            "/twirp/livekit.RoomService/DeleteRoom",
            Some(token.clone()),
            proto::DeleteRoomRequest {
                room: "missing".to_string(),
            },
        )
        .await;
        assert_twirp_error(missing, StatusCode::NOT_FOUND, "not_found").await;

        let created = post(
            app.clone(),
            "/twirp/livekit.RoomService/CreateRoom",
            Some(token.clone()),
            proto::CreateRoomRequest {
                name: "room-a".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(created.status(), StatusCode::OK);

        let first_delete = post(
            app.clone(),
            "/twirp/livekit.RoomService/DeleteRoom",
            Some(token.clone()),
            proto::DeleteRoomRequest {
                room: "room-a".to_string(),
            },
        )
        .await;
        assert_eq!(first_delete.status(), StatusCode::OK);

        let second_delete = post(
            app,
            "/twirp/livekit.RoomService/DeleteRoom",
            Some(token),
            proto::DeleteRoomRequest {
                room: "room-a".to_string(),
            },
        )
        .await;
        assert_twirp_error(second_delete, StatusCode::NOT_FOUND, "not_found").await;
    }

    #[tokio::test]
    async fn update_room_metadata_enforces_512_kib_limit() {
        let app = router(test_state());
        let admin = token(VideoGrants {
            room_create: true,
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let created = post(
            app.clone(),
            "/twirp/livekit.RoomService/CreateRoom",
            Some(admin.clone()),
            proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(created.status(), StatusCode::OK);

        let too_large = post(
            app.clone(),
            "/twirp/livekit.RoomService/UpdateRoomMetadata",
            Some(admin.clone()),
            proto::UpdateRoomMetadataRequest {
                room: "test-room".to_string(),
                metadata: "a".repeat(512 * 1024 + 1),
            },
        )
        .await;
        assert_twirp_error(too_large, StatusCode::BAD_REQUEST, "invalid_argument").await;

        let boundary = post(
            app,
            "/twirp/livekit.RoomService/UpdateRoomMetadata",
            Some(admin),
            proto::UpdateRoomMetadataRequest {
                room: "test-room".to_string(),
                metadata: "a".repeat(512 * 1024),
            },
        )
        .await;
        assert_eq!(boundary.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn update_subscriptions_invokes_runtime_when_changes_apply() {
        let runtime = Arc::new(RecordingRuntime::default());
        let state = test_state_with_runtime(Some(runtime.clone()));
        state
            .rooms
            .join_participant(
                "test-room",
                "publisher",
                "Publisher",
                String::new(),
                Default::default(),
            )
            .expect("publisher should join");
        state
            .rooms
            .join_participant(
                "test-room",
                "subscriber",
                "Subscriber",
                String::new(),
                Default::default(),
            )
            .expect("subscriber should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    ..Default::default()
                },
            )
            .expect("track should add");
        let app = router(state);

        let response = post(
            app,
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "subscriber".to_string(),
                track_sids: vec!["TR_test".to_string()],
                subscribe: false,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);

        let calls = runtime.calls.lock().expect("runtime calls should lock");
        assert!(calls.iter().any(|call| matches!(
            call,
            RuntimeCall::ApplyUpdateSubscriptions {
                room,
                identity,
                subscribe,
                track_sids,
                participant_tracks
            } if room == "test-room"
                && identity == "subscriber"
                && !subscribe
                && track_sids == &vec!["TR_test".to_string()]
                && *participant_tracks == 0
        )));
    }

    #[tokio::test]
    async fn perform_rpc_policy_is_stable_unimplemented() {
        let response = post(
            router(test_state()),
            "/twirp/livekit.RoomService/PerformRpc",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::PerformRpcRequest {
                room: "test-room".to_string(),
                destination_identity: "alice".to_string(),
                payload: "hello".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(response, StatusCode::NOT_IMPLEMENTED, "unimplemented").await;
    }

    #[tokio::test]
    async fn perform_rpc_returns_not_found_when_runtime_reports_missing_participant() {
        let runtime = Arc::new(RecordingRuntime::default());
        runtime
            .rpc_result
            .lock()
            .expect("rpc result lock should not be poisoned")
            .replace(Err(RoomStoreError::ParticipantNotFound));

        let state = test_state_with_runtime(Some(runtime));
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");

        let response = post(
            router(state),
            "/twirp/livekit.RoomService/PerformRpc",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::PerformRpcRequest {
                room: "test-room".to_string(),
                destination_identity: "missing".to_string(),
                method: "ping".to_string(),
                payload: "hello".to_string(),
                response_timeout_ms: 1_000,
            },
        )
        .await;
        assert_twirp_error(response, StatusCode::NOT_FOUND, "not_found").await;
    }

    #[tokio::test]
    async fn perform_rpc_invokes_runtime_and_returns_payload() {
        let runtime = Arc::new(RecordingRuntime::default());
        runtime
            .rpc_result
            .lock()
            .expect("rpc result lock should not be poisoned")
            .replace(Ok(proto::PerformRpcResponse {
                payload: "pong".to_string(),
            }));

        let state = test_state_with_runtime(Some(runtime.clone()));
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");

        let response = post(
            router(state),
            "/twirp/livekit.RoomService/PerformRpc",
            Some(token(VideoGrants {
                room_admin: true,
                room: "test-room".to_string(),
                ..Default::default()
            })),
            proto::PerformRpcRequest {
                room: "test-room".to_string(),
                destination_identity: "alice".to_string(),
                method: "ping".to_string(),
                payload: "hello".to_string(),
                response_timeout_ms: 4_000,
            },
        )
        .await;

        assert_eq!(response.status(), StatusCode::OK);
        let decoded = proto::PerformRpcResponse::decode(body_bytes(response).await)
            .expect("perform rpc response should decode");
        assert_eq!(decoded.payload, "pong");

        assert!(
            runtime
                .calls
                .lock()
                .expect("runtime calls lock should not be poisoned")
                .iter()
                .any(|call| matches!(
                    call,
                    RuntimeCall::PerformRpc {
                        room,
                        destination_identity,
                        method,
                        payload,
                        response_timeout_ms,
                    } if room == "test-room"
                        && destination_identity == "alice"
                        && method == "ping"
                        && payload == "hello"
                        && *response_timeout_ms == 4_000
                ))
        );
    }

    #[tokio::test]
    async fn room_service_all_methods_reject_missing_auth() {
        let app = router(test_state());

        let cases = vec![
            (
                "/twirp/livekit.RoomService/CreateRoom",
                proto::CreateRoomRequest {
                    name: "room-a".to_string(),
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
            (
                "/twirp/livekit.RoomService/ListRooms",
                proto::ListRoomsRequest::default().encode_to_vec(),
            ),
            (
                "/twirp/livekit.RoomService/DeleteRoom",
                proto::DeleteRoomRequest {
                    room: "room-a".to_string(),
                }
                .encode_to_vec(),
            ),
            (
                "/twirp/livekit.RoomService/ListParticipants",
                proto::ListParticipantsRequest {
                    room: "room-a".to_string(),
                }
                .encode_to_vec(),
            ),
            (
                "/twirp/livekit.RoomService/GetParticipant",
                proto::RoomParticipantIdentity {
                    room: "room-a".to_string(),
                    identity: "alice".to_string(),
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
            (
                "/twirp/livekit.RoomService/SendData",
                proto::SendDataRequest {
                    room: "room-a".to_string(),
                    data: b"hello".to_vec(),
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
            (
                "/twirp/livekit.RoomService/PerformRpc",
                proto::PerformRpcRequest {
                    room: "room-a".to_string(),
                    destination_identity: "alice".to_string(),
                    payload: "hello".to_string(),
                    ..Default::default()
                }
                .encode_to_vec(),
            ),
        ];

        for (path, bytes) in cases {
            let response = app
                .clone()
                .oneshot(
                    Request::builder()
                        .method("POST")
                        .uri(path)
                        .header(header::CONTENT_TYPE, APPLICATION_PROTOBUF)
                        .body(Body::from(bytes))
                        .expect("request should build"),
                )
                .await
                .expect("router should respond");
            assert_twirp_error(response, StatusCode::UNAUTHORIZED, "unauthenticated").await;
        }
    }

    #[tokio::test]
    async fn list_participants_and_get_participant_cover_not_found_and_fields() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                "meta-a".to_string(),
                std::collections::HashMap::from([("tier".to_string(), "gold".to_string())]),
            )
            .expect("participant should join");

        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let missing_room_admin = token(VideoGrants {
            room_admin: true,
            room: "missing".to_string(),
            ..Default::default()
        });
        let missing_room = post(
            app.clone(),
            "/twirp/livekit.RoomService/ListParticipants",
            Some(missing_room_admin.clone()),
            proto::ListParticipantsRequest {
                room: "missing".to_string(),
            },
        )
        .await;
        assert_twirp_error(missing_room, StatusCode::NOT_FOUND, "not_found").await;

        let list = post(
            app.clone(),
            "/twirp/livekit.RoomService/ListParticipants",
            Some(admin.clone()),
            proto::ListParticipantsRequest {
                room: "test-room".to_string(),
            },
        )
        .await;
        assert_eq!(list.status(), StatusCode::OK);
        let listed =
            proto::ListParticipantsResponse::decode(body_bytes(list).await).expect("list decode");
        assert_eq!(listed.participants.len(), 1);
        assert_eq!(listed.participants[0].identity, "alice");
        assert!(listed.participants[0].sid.starts_with("PA_"));
        assert_eq!(
            listed.participants[0].state,
            proto::participant_info::State::Joined as i32
        );
        assert_eq!(
            listed.participants[0].kind,
            proto::participant_info::Kind::Standard as i32
        );
        assert!(listed.participants[0].tracks.is_empty());

        let missing_room_get = post(
            app.clone(),
            "/twirp/livekit.RoomService/GetParticipant",
            Some(missing_room_admin),
            proto::RoomParticipantIdentity {
                room: "missing".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(missing_room_get, StatusCode::NOT_FOUND, "not_found").await;

        let get = post(
            app,
            "/twirp/livekit.RoomService/GetParticipant",
            Some(admin),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(get.status(), StatusCode::OK);
        let participant =
            proto::ParticipantInfo::decode(body_bytes(get).await).expect("participant decode");
        assert_eq!(participant.identity, "alice");
        assert!(participant.sid.starts_with("PA_"));
        assert_eq!(participant.name, "Alice");
        assert_eq!(participant.metadata, "meta-a");
        assert_eq!(
            participant.state,
            proto::participant_info::State::Joined as i32
        );
        assert_eq!(
            participant.kind,
            proto::participant_info::Kind::Standard as i32
        );
        assert!(participant.permission.is_none());
        assert_eq!(
            participant.attributes.get("tier"),
            Some(&"gold".to_string())
        );
        assert!(participant.joined_at > 0);
    }

    #[tokio::test]
    async fn list_participants_includes_hidden_participants_for_admin() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "visible",
                "Visible",
                String::new(),
                Default::default(),
            )
            .expect("visible participant should join");
        state
            .rooms
            .join_participant_with_permission(
                "test-room",
                "hidden",
                "Hidden",
                String::new(),
                Default::default(),
                Some(proto::ParticipantPermission {
                    hidden: true,
                    ..Default::default()
                }),
            )
            .expect("hidden participant should join");

        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let response = post(
            app,
            "/twirp/livekit.RoomService/ListParticipants",
            Some(admin),
            proto::ListParticipantsRequest {
                room: "test-room".to_string(),
            },
        )
        .await;
        assert_eq!(response.status(), StatusCode::OK);
        let listed = proto::ListParticipantsResponse::decode(body_bytes(response).await)
            .expect("list response should decode");

        let mut identities = listed
            .participants
            .iter()
            .map(|participant| participant.identity.clone())
            .collect::<Vec<_>>();
        identities.sort();
        assert_eq!(
            identities,
            vec!["hidden".to_string(), "visible".to_string()]
        );
    }

    #[tokio::test]
    async fn remove_participant_and_mute_track_not_found_and_state_paths() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "alice",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    muted: false,
                    ..Default::default()
                },
            )
            .expect("track should add");
        let rooms = state.rooms.clone();
        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let missing_room_admin = token(VideoGrants {
            room_admin: true,
            room: "missing".to_string(),
            ..Default::default()
        });
        let mute_missing_room = post(
            app.clone(),
            "/twirp/livekit.RoomService/MutePublishedTrack",
            Some(missing_room_admin.clone()),
            proto::MuteRoomTrackRequest {
                room: "missing".to_string(),
                identity: "alice".to_string(),
                track_sid: "TR_test".to_string(),
                muted: true,
            },
        )
        .await;
        assert_twirp_error(mute_missing_room, StatusCode::NOT_FOUND, "not_found").await;

        let mute_missing_participant = post(
            app.clone(),
            "/twirp/livekit.RoomService/MutePublishedTrack",
            Some(admin.clone()),
            proto::MuteRoomTrackRequest {
                room: "test-room".to_string(),
                identity: "missing".to_string(),
                track_sid: "TR_test".to_string(),
                muted: true,
            },
        )
        .await;
        assert_twirp_error(mute_missing_participant, StatusCode::NOT_FOUND, "not_found").await;

        let mute_missing_track = post(
            app.clone(),
            "/twirp/livekit.RoomService/MutePublishedTrack",
            Some(admin.clone()),
            proto::MuteRoomTrackRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                track_sid: "TR_missing".to_string(),
                muted: true,
            },
        )
        .await;
        assert_twirp_error(mute_missing_track, StatusCode::NOT_FOUND, "not_found").await;

        let mute_ok = post(
            app.clone(),
            "/twirp/livekit.RoomService/MutePublishedTrack",
            Some(admin.clone()),
            proto::MuteRoomTrackRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                track_sid: "TR_test".to_string(),
                muted: true,
            },
        )
        .await;
        assert_eq!(mute_ok.status(), StatusCode::OK);
        let participant = rooms
            .get_participant("test-room", "alice")
            .expect("participant should exist");
        assert!(participant.tracks[0].muted);

        let remove_missing_room = post(
            app.clone(),
            "/twirp/livekit.RoomService/RemoveParticipant",
            Some(missing_room_admin),
            proto::RoomParticipantIdentity {
                room: "missing".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(remove_missing_room, StatusCode::NOT_FOUND, "not_found").await;

        let remove_missing_participant = post(
            app.clone(),
            "/twirp/livekit.RoomService/RemoveParticipant",
            Some(admin.clone()),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "missing".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(
            remove_missing_participant,
            StatusCode::NOT_FOUND,
            "not_found",
        )
        .await;

        let remove_ok = post(
            app,
            "/twirp/livekit.RoomService/RemoveParticipant",
            Some(admin),
            proto::RoomParticipantIdentity {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(remove_ok.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn update_participant_and_subscriptions_not_found_and_update_paths() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .join_participant(
                "test-room",
                "publisher",
                "Publisher",
                String::new(),
                Default::default(),
            )
            .expect("publisher should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "publisher",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    ..Default::default()
                },
            )
            .expect("track should add");
        let rooms = state.rooms.clone();
        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let missing_room_admin = token(VideoGrants {
            room_admin: true,
            room: "missing".to_string(),
            ..Default::default()
        });
        let missing_room = post(
            app.clone(),
            "/twirp/livekit.RoomService/UpdateParticipant",
            Some(missing_room_admin.clone()),
            proto::UpdateParticipantRequest {
                room: "missing".to_string(),
                identity: "alice".to_string(),
                metadata: "m".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(missing_room, StatusCode::NOT_FOUND, "not_found").await;

        let missing_participant = post(
            app.clone(),
            "/twirp/livekit.RoomService/UpdateParticipant",
            Some(admin.clone()),
            proto::UpdateParticipantRequest {
                room: "test-room".to_string(),
                identity: "missing".to_string(),
                metadata: "m".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(missing_participant, StatusCode::NOT_FOUND, "not_found").await;

        let updated = post(
            app.clone(),
            "/twirp/livekit.RoomService/UpdateParticipant",
            Some(admin.clone()),
            proto::UpdateParticipantRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                metadata: "meta-updated".to_string(),
                name: "Alice Updated".to_string(),
                attributes: std::collections::HashMap::from([(
                    "role".to_string(),
                    "speaker".to_string(),
                )]),
                permission: Some(proto::ParticipantPermission {
                    can_subscribe: false,
                    can_publish: true,
                    ..Default::default()
                }),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(updated.status(), StatusCode::OK);
        let participant = rooms
            .get_participant("test-room", "alice")
            .expect("participant should exist");
        assert_eq!(participant.metadata, "meta-updated");
        assert_eq!(participant.name, "Alice Updated");
        assert_eq!(
            participant.attributes.get("role"),
            Some(&"speaker".to_string())
        );
        assert_eq!(participant.permission.map(|p| p.can_subscribe), Some(false));

        let missing_room_subs = post(
            app.clone(),
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(missing_room_admin),
            proto::UpdateSubscriptionsRequest {
                room: "missing".to_string(),
                identity: "alice".to_string(),
                track_sids: vec!["TR_test".to_string()],
                subscribe: false,
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(missing_room_subs, StatusCode::NOT_FOUND, "not_found").await;

        let unknown_track = post(
            app,
            "/twirp/livekit.RoomService/UpdateSubscriptions",
            Some(admin),
            proto::UpdateSubscriptionsRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                track_sids: vec!["TR_unknown".to_string()],
                subscribe: false,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(unknown_track.status(), StatusCode::OK);
    }

    #[tokio::test]
    async fn mute_published_track_rejects_unmute_when_remote_unmute_disabled_by_default() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");
        state
            .rooms
            .add_participant_track(
                "test-room",
                "alice",
                proto::TrackInfo {
                    sid: "TR_test".to_string(),
                    muted: true,
                    ..Default::default()
                },
            )
            .expect("track should add");

        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let response = post(
            app,
            "/twirp/livekit.RoomService/MutePublishedTrack",
            Some(admin),
            proto::MuteRoomTrackRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                track_sid: "TR_test".to_string(),
                muted: false,
            },
        )
        .await;
        assert_twirp_error(response, StatusCode::BAD_REQUEST, "invalid_argument").await;
    }

    #[tokio::test]
    async fn update_participant_rejects_metadata_over_512_kib() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");

        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let oversized = "m".repeat(512 * 1024 + 1);
        let response = post(
            app,
            "/twirp/livekit.RoomService/UpdateParticipant",
            Some(admin),
            proto::UpdateParticipantRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                metadata: oversized,
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(response, StatusCode::BAD_REQUEST, "invalid_argument").await;
    }

    #[tokio::test]
    async fn update_participant_rejects_attributes_over_64_kib() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "test-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("participant should join");

        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let response = post(
            app,
            "/twirp/livekit.RoomService/UpdateParticipant",
            Some(admin),
            proto::UpdateParticipantRequest {
                room: "test-room".to_string(),
                identity: "alice".to_string(),
                attributes: std::collections::HashMap::from([(
                    "k".to_string(),
                    "v".repeat(64 * 1024),
                )]),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(response, StatusCode::BAD_REQUEST, "invalid_argument").await;
    }

    #[tokio::test]
    async fn agent_dispatch_create_list_delete_and_limits() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let too_large_metadata = post(
            app.clone(),
            "/twirp/livekit.AgentDispatchService/CreateDispatch",
            Some(admin.clone()),
            proto::CreateAgentDispatchRequest {
                room: "test-room".to_string(),
                metadata: "m".repeat(512 * 1024 + 1),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(
            too_large_metadata,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
        )
        .await;

        let too_large_attributes = post(
            app.clone(),
            "/twirp/livekit.AgentDispatchService/CreateDispatch",
            Some(admin.clone()),
            proto::CreateAgentDispatchRequest {
                room: "test-room".to_string(),
                attributes: std::collections::HashMap::from([(
                    "k".to_string(),
                    "v".repeat(64 * 1024),
                )]),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(
            too_large_attributes,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
        )
        .await;

        let create = post(
            app.clone(),
            "/twirp/livekit.AgentDispatchService/CreateDispatch",
            Some(admin.clone()),
            proto::CreateAgentDispatchRequest {
                agent_name: "ag1".to_string(),
                room: "test-room".to_string(),
                metadata: "md".to_string(),
                deployment: "prod".to_string(),
                attributes: std::collections::HashMap::from([(
                    "tier".to_string(),
                    "gold".to_string(),
                )]),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(create.status(), StatusCode::OK);
        let created = proto::AgentDispatch::decode(body_bytes(create).await)
            .expect("agent dispatch should decode");
        assert!(!created.id.is_empty());
        assert_eq!(created.room, "test-room");
        assert_eq!(created.agent_name, "ag1");

        let list = post(
            app.clone(),
            "/twirp/livekit.AgentDispatchService/ListDispatch",
            Some(admin.clone()),
            proto::ListAgentDispatchRequest {
                room: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(list.status(), StatusCode::OK);
        let listed = proto::ListAgentDispatchResponse::decode(body_bytes(list).await)
            .expect("list dispatch response should decode");
        assert_eq!(listed.agent_dispatches.len(), 1);
        assert_eq!(listed.agent_dispatches[0].id, created.id);

        let delete = post(
            app.clone(),
            "/twirp/livekit.AgentDispatchService/DeleteDispatch",
            Some(admin.clone()),
            proto::DeleteAgentDispatchRequest {
                dispatch_id: created.id.clone(),
                room: "test-room".to_string(),
            },
        )
        .await;
        assert_eq!(delete.status(), StatusCode::OK);
        let deleted = proto::AgentDispatch::decode(body_bytes(delete).await)
            .expect("deleted dispatch should decode");
        assert_eq!(deleted.id, created.id);
        assert!(
            deleted
                .state
                .as_ref()
                .is_some_and(|state| state.deleted_at > 0)
        );

        let list_after_delete = post(
            app,
            "/twirp/livekit.AgentDispatchService/ListDispatch",
            Some(admin),
            proto::ListAgentDispatchRequest {
                room: "test-room".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_eq!(list_after_delete.status(), StatusCode::OK);
        let listed_after_delete =
            proto::ListAgentDispatchResponse::decode(body_bytes(list_after_delete).await)
                .expect("list dispatch response should decode");
        assert!(listed_after_delete.agent_dispatches.is_empty());
    }

    #[tokio::test]
    async fn send_data_kind_and_perform_rpc_validation_paths() {
        let state = test_state();
        state
            .rooms
            .create_room(proto::CreateRoomRequest {
                name: "test-room".to_string(),
                ..Default::default()
            })
            .expect("room should create");
        let app = router(state);
        let admin = token(VideoGrants {
            room_admin: true,
            room: "test-room".to_string(),
            ..Default::default()
        });

        let reliable = post(
            app.clone(),
            "/twirp/livekit.RoomService/SendData",
            Some(admin.clone()),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"r".to_vec(),
                kind: proto::data_packet::Kind::Reliable as i32,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(reliable.status(), StatusCode::OK);

        let lossy = post(
            app.clone(),
            "/twirp/livekit.RoomService/SendData",
            Some(admin.clone()),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"l".to_vec(),
                kind: proto::data_packet::Kind::Lossy as i32,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(lossy.status(), StatusCode::OK);

        let unknown_kind = post(
            app.clone(),
            "/twirp/livekit.RoomService/SendData",
            Some(admin.clone()),
            proto::SendDataRequest {
                room: "test-room".to_string(),
                data: b"u".to_vec(),
                kind: 999,
                ..Default::default()
            },
        )
        .await;
        assert_eq!(
            unknown_kind.status(),
            StatusCode::OK,
            "unknown kind currently follows stable fallback behavior"
        );

        let missing_destination = post(
            app.clone(),
            "/twirp/livekit.RoomService/PerformRpc",
            Some(admin.clone()),
            proto::PerformRpcRequest {
                room: "test-room".to_string(),
                destination_identity: String::new(),
                payload: "hello".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(
            missing_destination,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
        )
        .await;

        let wrong_room = post(
            app.clone(),
            "/twirp/livekit.RoomService/PerformRpc",
            Some(admin.clone()),
            proto::PerformRpcRequest {
                room: "other-room".to_string(),
                destination_identity: "alice".to_string(),
                payload: "hello".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(wrong_room, StatusCode::FORBIDDEN, "permission_denied").await;

        let too_long_method = post(
            app.clone(),
            "/twirp/livekit.RoomService/PerformRpc",
            Some(admin.clone()),
            proto::PerformRpcRequest {
                room: "test-room".to_string(),
                destination_identity: "alice".to_string(),
                method: "m".repeat(65),
                payload: "hello".to_string(),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(too_long_method, StatusCode::BAD_REQUEST, "invalid_argument").await;

        let too_large_payload = post(
            app,
            "/twirp/livekit.RoomService/PerformRpc",
            Some(admin),
            proto::PerformRpcRequest {
                room: "test-room".to_string(),
                destination_identity: "alice".to_string(),
                payload: "x".repeat(15 * 1024 + 1),
                ..Default::default()
            },
        )
        .await;
        assert_twirp_error(
            too_large_payload,
            StatusCode::BAD_REQUEST,
            "invalid_argument",
        )
        .await;
    }
}
