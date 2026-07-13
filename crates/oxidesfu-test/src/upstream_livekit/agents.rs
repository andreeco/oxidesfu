use std::sync::{
    Arc,
    atomic::{AtomicU32, Ordering},
};

use super::*;
use super::native_rtc::{NativeMediaParticipant, RawDataTopology};

struct NativeAgentClient {
    write_tx: tokio::sync::mpsc::UnboundedSender<Message>,
    close_tx: tokio::sync::oneshot::Sender<()>,
    _task: tokio::task::JoinHandle<()>,
    registered: Arc<AtomicU32>,
    room_jobs: Arc<AtomicU32>,
    publisher_jobs: Arc<AtomicU32>,
    participant_jobs: Arc<AtomicU32>,
    requested_jobs_rx: tokio::sync::Mutex<tokio::sync::mpsc::UnboundedReceiver<proto::Job>>,
}

impl NativeAgentClient {
    async fn connect(base_url: &str, token: &str) -> Self {
        let ws_base = base_url.replace("http://", "ws://");
        let mut request = format!("{ws_base}/agent")
            .into_client_request()
            .expect("agent websocket request should build");
        request.headers_mut().insert(
            "Authorization",
            HeaderValue::from_str(&format!("Bearer {token}"))
                .expect("authorization header should be valid"),
        );

        let (socket, response) = connect_async(request)
            .await
            .expect("agent websocket connect should succeed");
        assert_eq!(response.status(), 101);

        let (mut ws_write, mut ws_read) = socket.split();
        let (write_tx, mut write_rx) = tokio::sync::mpsc::unbounded_channel::<Message>();
        let (close_tx, mut close_rx) = tokio::sync::oneshot::channel::<()>();

        let registered = Arc::new(AtomicU32::new(0));
        let room_jobs = Arc::new(AtomicU32::new(0));
        let publisher_jobs = Arc::new(AtomicU32::new(0));
        let participant_jobs = Arc::new(AtomicU32::new(0));
        let (requested_jobs_tx, requested_jobs_rx) = tokio::sync::mpsc::unbounded_channel::<proto::Job>();

        let registered_clone = registered.clone();
        let room_jobs_clone = room_jobs.clone();
        let publisher_jobs_clone = publisher_jobs.clone();
        let participant_jobs_clone = participant_jobs.clone();

        let task = tokio::spawn(async move {
            loop {
                tokio::select! {
                    Some(message) = write_rx.recv() => {
                        if ws_write.send(message).await.is_err() {
                            break;
                        }
                    }
                    message = ws_read.next() => {
                        let Some(Ok(message)) = message else {
                            break;
                        };
                        let Message::Binary(bytes) = message else {
                            continue;
                        };
                        let Ok(server_message) = proto::ServerMessage::decode(bytes.as_ref()) else {
                            break;
                        };
                        match server_message.message {
                            Some(proto::server_message::Message::Register(_)) => {
                                registered_clone.fetch_add(1, Ordering::Relaxed);
                            }
                            Some(proto::server_message::Message::Availability(request)) => {
                                let Some(job) = request.job else {
                                    continue;
                                };
                                let _ = requested_jobs_tx.send(job.clone());
                                let response = proto::WorkerMessage {
                                    message: Some(proto::worker_message::Message::Availability(
                                        proto::AvailabilityResponse {
                                            job_id: job.id,
                                            available: true,
                                            ..Default::default()
                                        },
                                    )),
                                };
                                let _ = ws_write.send(Message::Binary(response.encode_to_vec().into())).await;
                            }
                            Some(proto::server_message::Message::Assignment(assignment)) => {
                                let Some(job) = assignment.job else {
                                    continue;
                                };
                                let Some(job_type) = proto::JobType::try_from(job.r#type).ok() else {
                                    continue;
                                };
                                match job_type {
                                    proto::JobType::JtRoom => { room_jobs_clone.fetch_add(1, Ordering::Relaxed); }
                                    proto::JobType::JtPublisher => { publisher_jobs_clone.fetch_add(1, Ordering::Relaxed); }
                                    proto::JobType::JtParticipant => { participant_jobs_clone.fetch_add(1, Ordering::Relaxed); }
                                }
                            }
                            _ => {}
                        }
                    }
                    _ = &mut close_rx => {
                        let _ = ws_write.close().await;
                        break;
                    }
                }
            }
        });

        Self {
            write_tx,
            close_tx,
            _task: task,
            registered,
            room_jobs,
            publisher_jobs,
            participant_jobs,
            requested_jobs_rx: tokio::sync::Mutex::new(requested_jobs_rx),
        }
    }

    fn run(&self, job_type: proto::JobType, namespace: &str) {
        let register = proto::WorkerMessage {
            message: Some(proto::worker_message::Message::Register(
                proto::RegisterWorkerRequest {
                    r#type: job_type as i32,
                    version: "version".to_string(),
                    namespace: Some(namespace.to_string()),
                    ..Default::default()
                },
            )),
        };
        let _ = self
            .write_tx
            .send(Message::Binary(register.encode_to_vec().into()));

    }

    async fn next_requested_job(&self) -> Option<proto::Job> {
        self.requested_jobs_rx.lock().await.recv().await
    }

    fn registered_count(&self) -> u32 {
        self.registered.load(Ordering::Relaxed)
    }

    fn room_jobs_count(&self) -> u32 {
        self.room_jobs.load(Ordering::Relaxed)
    }

    fn publisher_jobs_count(&self) -> u32 {
        self.publisher_jobs.load(Ordering::Relaxed)
    }

    fn participant_jobs_count(&self) -> u32 {
        self.participant_jobs.load(Ordering::Relaxed)
    }

    async fn close(self) {
        let _ = self.close_tx.send(());
    }
}

fn agent_worker_token() -> String {
    let mut keys = oxidesfu_auth::ApiKeyStore::new();
    keys.insert(API_KEY, API_SECRET);
    let verifier = oxidesfu_auth::TokenVerifier::new(keys);

    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .expect("system clock should be after epoch")
        .as_secs() as usize;
    let claims = oxidesfu_auth::Claims {
        iss: API_KEY.to_string(),
        exp: now + Duration::from_secs(60).as_secs() as usize,
        nbf: now.saturating_sub(1),

        video: oxidesfu_auth::VideoGrants {
            agent: true,
            ..Default::default()
        },
        ..Default::default()
    };

    verifier
        .issue_token(API_KEY, &claims)
        .expect("agent token should encode")
}

async fn wait_for_registered(agent: &NativeAgentClient) {
    tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            if agent.registered_count() >= 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("worker should register before timeout");
}

// Upstream: livekit/test/agent_test.go::TestAgents
#[tokio::test]
async fn test_agents() {
    use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;

    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        let (addr, server) = spawn_single_node().await;
        let url = base_url(addr);

        let ac1 = NativeAgentClient::connect(&url, &agent_worker_token()).await;
        let ac2 = NativeAgentClient::connect(&url, &agent_worker_token()).await;
        let ac3 = NativeAgentClient::connect(&url, &agent_worker_token()).await;
        let ac4 = NativeAgentClient::connect(&url, &agent_worker_token()).await;
        let ac5 = NativeAgentClient::connect(&url, &agent_worker_token()).await;
        let ac6 = NativeAgentClient::connect(&url, &agent_worker_token()).await;

        ac1.run(proto::JobType::JtRoom, "default");
        ac2.run(proto::JobType::JtRoom, "default");
        ac3.run(proto::JobType::JtPublisher, "default");
        ac4.run(proto::JobType::JtPublisher, "default");
        ac5.run(proto::JobType::JtParticipant, "default");
        ac6.run(proto::JobType::JtParticipant, "default");

        for ac in [&ac1, &ac2, &ac3, &ac4, &ac5, &ac6] {
            wait_for_registered(ac).await;
        }

        let room = format!("upstream-agent-{}-{}", topology.name(), unique_suffix());
        let mut c1 = NativeMediaParticipant::connect(topology, addr, &room, "c1").await;
        let mut c2 = NativeMediaParticipant::connect(topology, addr, &room, "c2").await;
        let _c1_audio = c1
            .publish_track("c1-audio", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;
        let _c1_video = c1
            .publish_track("c1-video", "video", RtpCodecKind::Video, "video/vp8")
            .await;

        tokio::time::timeout(Duration::from_secs(6), async {
            loop {
                let room_jobs = ac1.room_jobs_count() + ac2.room_jobs_count();
                let publisher_jobs = ac3.publisher_jobs_count() + ac4.publisher_jobs_count();
                let participant_jobs = ac5.participant_jobs_count() + ac6.participant_jobs_count();
                if room_jobs == 1 && publisher_jobs == 1 && participant_jobs == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{}: initial room/publisher/participant jobs should be assigned", topology.name()));

        let _c2_audio = c2
            .publish_track("c2-audio", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;
        let _c2_video = c2
            .publish_track("c2-video", "video", RtpCodecKind::Video, "video/vp8")
            .await;

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let room_jobs = ac1.room_jobs_count() + ac2.room_jobs_count();
                let publisher_jobs = ac3.publisher_jobs_count() + ac4.publisher_jobs_count();
                let participant_jobs = ac5.participant_jobs_count() + ac6.participant_jobs_count();
                if room_jobs == 1 && publisher_jobs == 2 && participant_jobs == 2 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{}: subsequent publisher jobs should be assigned without duplicating room/participant jobs", topology.name()));

        ac1.close().await;
        ac2.close().await;
        ac3.close().await;
        ac4.close().await;
        ac5.close().await;
        ac6.close().await;
        server.abort();
    }
}

// Upstream: livekit/test/agent_test.go::TestAgentNamespaces
#[tokio::test]
async fn test_agent_namespaces() {
    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        let (addr, server) = spawn_single_node().await;
        let url = base_url(addr);

        let ac1 = NativeAgentClient::connect(&url, &agent_worker_token()).await;
        let ac2 = NativeAgentClient::connect(&url, &agent_worker_token()).await;
        ac1.run(proto::JobType::JtRoom, "namespace1");
        ac2.run(proto::JobType::JtRoom, "namespace2");

        let room = format!("upstream-agent-ns-{}-{}", topology.name(), unique_suffix());
        let create_room_token = AccessToken::with_api_key(API_KEY, API_SECRET)
            .with_identity("room-admin")
            .with_name("room-admin")
            .with_grants(VideoGrants {
                room_create: true,
                room_admin: true,
                room: room.clone(),
                ..Default::default()
            })
            .to_jwt()
            .expect("create-room token should encode");
        let create_room_request = proto::CreateRoomRequest {
            name: room.clone(),
            agents: vec![
                proto::RoomAgentDispatch::default(),
                proto::RoomAgentDispatch {
                    agent_name: "ag".to_string(),
                    ..Default::default()
                },
            ],
            ..Default::default()
        };
        let create_room_response = http_post_status_and_body(
            &url,
            "/twirp/livekit.RoomService/CreateRoom",
            "application/protobuf",
            Some(&format!("Bearer {create_room_token}")),
            &create_room_request.encode_to_vec(),
        )
        .await;
        assert_eq!(
            create_room_response.status, 200,
            "{}: create room should succeed: {}",
            topology.name(),
            create_room_response.body
        );

        wait_for_registered(&ac1).await;
        wait_for_registered(&ac2).await;
        let _c1 = NativeMediaParticipant::connect(topology, addr, &room, "c1").await;

        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if ac1.room_jobs_count() == 1 && ac2.room_jobs_count() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{}: both namespaced room workers should receive one room job", topology.name()));

        let job1 = ac1
            .next_requested_job()
            .await
            .expect("namespace1 worker should receive availability request");
        let job2 = ac2
            .next_requested_job()
            .await
            .expect("namespace2 worker should receive availability request");
        assert_eq!(job1.namespace, "namespace1");
        assert_eq!(job2.namespace, "namespace2");
        assert_ne!(job1.id, job2.id);

        ac1.close().await;
        ac2.close().await;
        server.abort();
    }
}

// Upstream: livekit/test/agent_test.go::TestAgentMultiNode
#[tokio::test]
async fn test_agent_multi_node() {
    use rtc::rtp_transceiver::rtp_sender::RtpCodecKind;

    for topology in [
        RawDataTopology::V0DualPeerConnection,
        RawDataTopology::V0SinglePeerConnection,
        RawDataTopology::V1,
    ] {
        let Some((mut redis, mut node_a, mut node_b, redis_url, node_a_url, node_b_url)) =
            spawn_two_process_nodes().await
        else {
            return;
        };

        let ac1 = NativeAgentClient::connect(&node_a_url, &agent_worker_token()).await;
        let ac2 = NativeAgentClient::connect(&node_a_url, &agent_worker_token()).await;
        ac1.run(proto::JobType::JtRoom, "default");
        ac2.run(proto::JobType::JtPublisher, "default");
        wait_for_registered(&ac1).await;
        wait_for_registered(&ac2).await;

        let room = format!("upstream-agent-multinode-{}-{}", topology.name(), unique_suffix());
        let node_a_port = node_a_url
            .rsplit(':')
            .next()
            .and_then(|port| port.parse::<u16>().ok())
            .expect("node-a base url should include a port");
        force_room_assignment_to_node(
            &redis_url,
            &room,
            &format!("oxidesfu-local-{node_a_port}"),
        )
        .expect("room assignment should target node A before room creation from node B");

        let node_b_addr = node_b_url
            .strip_prefix("http://")
            .expect("node-b URL should be HTTP")
            .parse()
            .expect("node-b URL should contain a socket address");
        let mut c1 = NativeMediaParticipant::connect(topology, node_b_addr, &room, "c1").await;
        let _audio = c1
            .publish_track("audio", "audio", RtpCodecKind::Audio, "audio/opus")
            .await;

        tokio::time::sleep(Duration::from_secs(10)).await;
        tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                if ac1.room_jobs_count() == 1 && ac2.publisher_jobs_count() == 1 {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
        })
        .await
        .unwrap_or_else(|_| panic!("{}: node-A workers should receive room/publisher jobs for the node-B room", topology.name()));

        ac1.close().await;
        ac2.close().await;
        let _ = node_a.kill().await;
        let _ = node_b.kill().await;
        let _ = redis.kill().await;
    }
}
