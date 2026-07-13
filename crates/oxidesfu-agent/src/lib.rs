use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    thread,
    time::{Duration, SystemTime, UNIX_EPOCH},
};

use axum::{
    Router,
    extract::{
        Query, State, WebSocketUpgrade,
        ws::{Message as WsMessage, WebSocket},
    },
    http::{HeaderMap, StatusCode, header},
    response::{IntoResponse, Response},
    routing::get,
};
use futures_util::{SinkExt, StreamExt};
use livekit_protocol as proto;
use oxidesfu_auth::{AuthContext, AuthError, Claims, TokenVerifier, VideoGrants};
use prost::Message;
use serde::Deserialize;
use tokio::sync::{Mutex, mpsc, oneshot};

const AGENT_NAME_ATTRIBUTE_KEY: &str = "lk.agent_name";

const ASSIGN_JOB_TIMEOUT: Duration = Duration::from_secs(3);
const REDIS_AGENT_WEBHOOK_CHANNEL: &str = "oxidesfu:agent:webhook_events";

#[derive(Clone)]
pub struct AgentState {
    auth: TokenVerifier,
    rooms: oxidesfu_room::RoomStore,
    server_info: proto::ServerInfo,
    next_worker_id: Arc<AtomicU64>,
    next_job_id: Arc<AtomicU64>,
    next_dispatch_id: Arc<AtomicU64>,
    runtime: Arc<RuntimeState>,
    redis_webhook_relay: Option<RedisWebhookRelay>,
}

#[derive(Debug)]
struct RuntimeState {
    workers: Mutex<HashMap<String, RegisteredWorker>>,
    room_jobs_dispatched: Mutex<HashSet<String>>,
    participant_jobs_dispatched: Mutex<HashSet<(String, String)>>,
    publisher_jobs_dispatched: Mutex<HashSet<(String, String)>>,
}

impl RuntimeState {
    async fn release_departed_participant_jobs(
        &self,
        room_name: &str,
        participant_identity: &str,
        room_is_empty: bool,
    ) {
        let participant_key = (room_name.to_string(), participant_identity.to_string());
        self.participant_jobs_dispatched
            .lock()
            .await
            .remove(&participant_key);
        self.publisher_jobs_dispatched
            .lock()
            .await
            .remove(&participant_key);

        if room_is_empty {
            self.release_room_jobs(room_name).await;
        }
    }

    async fn release_room_jobs(&self, room_name: &str) {
        self.room_jobs_dispatched.lock().await.remove(room_name);
        self.participant_jobs_dispatched
            .lock()
            .await
            .retain(|(candidate_room, _)| candidate_room != room_name);
        self.publisher_jobs_dispatched
            .lock()
            .await
            .retain(|(candidate_room, _)| candidate_room != room_name);
    }

    async fn release_jobs_for_type(&self, job_type: proto::JobType) {
        match job_type {
            proto::JobType::JtRoom => self.room_jobs_dispatched.lock().await.clear(),
            proto::JobType::JtPublisher => self.publisher_jobs_dispatched.lock().await.clear(),
            proto::JobType::JtParticipant => self.participant_jobs_dispatched.lock().await.clear(),
        }
    }
}

#[derive(Debug)]
struct RegisteredWorker {
    id: String,
    api_key: String,
    job_type: i32,
    namespace: String,
    agent_name: String,
    deployment: String,
    permissions: proto::ParticipantPermission,
    status: i32,
    load: f32,
    outbound: mpsc::UnboundedSender<proto::ServerMessage>,
    pending_availability: HashMap<String, oneshot::Sender<proto::AvailabilityResponse>>,
}

#[derive(Debug, Clone)]
struct DispatchCandidate {
    worker_id: String,
    api_key: String,
    permissions: proto::ParticipantPermission,
    outbound: mpsc::UnboundedSender<proto::ServerMessage>,
}

#[derive(Debug, Clone)]
struct RedisWebhookRelay {
    redis_url: Arc<String>,
}

impl AgentState {
    pub fn new(auth: TokenVerifier, rooms: oxidesfu_room::RoomStore) -> Self {
        let state = Self {
            auth,
            rooms,
            server_info: proto::ServerInfo {
                edition: proto::server_info::Edition::Standard as i32,
                version: "oxidesfu".to_string(),
                protocol: 17,
                region: String::new(),
                node_id: "oxidesfu".to_string(),
                debug_info: String::new(),
                agent_protocol: 1,
            },
            next_worker_id: Arc::new(AtomicU64::new(1)),
            next_job_id: Arc::new(AtomicU64::new(1)),
            next_dispatch_id: Arc::new(AtomicU64::new(1)),
            runtime: Arc::new(RuntimeState {
                workers: Mutex::new(HashMap::new()),
                room_jobs_dispatched: Mutex::new(HashSet::new()),
                participant_jobs_dispatched: Mutex::new(HashSet::new()),
                publisher_jobs_dispatched: Mutex::new(HashSet::new()),
            }),
            redis_webhook_relay: None,
        };
        state.spawn_state_reconciler();
        state
    }

    pub fn with_redis_webhook_relay(mut self, redis_url: impl Into<String>) -> Self {
        let relay = RedisWebhookRelay {
            redis_url: Arc::new(redis_url.into()),
        };
        self.spawn_redis_webhook_subscriber(relay.clone());
        self.redis_webhook_relay = Some(relay);
        self
    }

    pub fn with_server_info(mut self, server_info: proto::ServerInfo) -> Self {
        self.server_info = server_info;
        self
    }

    fn next_worker_id(&self) -> String {
        format!(
            "AW_{:016x}",
            self.next_worker_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn next_job_id(&self) -> String {
        format!(
            "AJ_{:016x}",
            self.next_job_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    fn next_dispatch_id(&self) -> String {
        format!(
            "AD_{:016x}",
            self.next_dispatch_id.fetch_add(1, Ordering::Relaxed)
        )
    }

    pub fn signal_webhook_handler(&self) -> Arc<dyn Fn(proto::WebhookEvent) + Send + Sync> {
        let state = self.clone();
        Arc::new(move |event: proto::WebhookEvent| {
            if let Some(relay) = state.redis_webhook_relay.clone() {
                relay.publish(event.encode_to_vec());
            }
            let state = state.clone();
            tokio::spawn(async move {
                state.handle_webhook_event(event).await;
            });
        })
    }

    fn spawn_redis_webhook_subscriber(&self, relay: RedisWebhookRelay) {
        let (events_tx, mut events_rx) = mpsc::unbounded_channel::<proto::WebhookEvent>();
        let state = self.clone();
        tokio::spawn(async move {
            while let Some(event) = events_rx.recv().await {
                state.handle_webhook_event(event).await;
            }
        });

        let redis_url = relay.redis_url.clone();
        tokio::task::spawn_blocking(move || {
            loop {
                let client = match redis::Client::open(redis_url.as_ref().as_str()) {
                    Ok(client) => client,
                    Err(_) => {
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                };

                let mut connection = match client.get_connection() {
                    Ok(connection) => connection,
                    Err(_) => {
                        thread::sleep(Duration::from_millis(250));
                        continue;
                    }
                };

                let mut pubsub = connection.as_pubsub();
                if pubsub.subscribe(REDIS_AGENT_WEBHOOK_CHANNEL).is_err() {
                    thread::sleep(Duration::from_millis(250));
                    continue;
                }

                loop {
                    let message = match pubsub.get_message() {
                        Ok(message) => message,
                        Err(_) => break,
                    };
                    let payload = match message.get_payload::<Vec<u8>>() {
                        Ok(payload) => payload,
                        Err(_) => continue,
                    };
                    let Ok(event) = proto::WebhookEvent::decode(payload.as_slice()) else {
                        continue;
                    };
                    if events_tx.send(event).is_err() {
                        return;
                    }
                }

                thread::sleep(Duration::from_millis(250));
            }
        });
    }

    async fn handle_webhook_event(&self, event: proto::WebhookEvent) {
        let room = event.room;
        let participant = event.participant;

        match event.event.as_str() {
            "room_started" => {
                let Some(room) = room else {
                    return;
                };
                self.try_dispatch_room_job(room.name).await;
            }
            "participant_joined" => {
                let Some(room) = room else {
                    return;
                };
                let Some(participant) = participant else {
                    return;
                };
                self.try_dispatch_participant_job(
                    room.name,
                    participant.identity,
                    participant.name,
                )
                .await;
            }
            "track_published" => {
                let Some(room) = room else {
                    return;
                };
                let Some(participant) = participant else {
                    return;
                };
                self.try_dispatch_publisher_job(room.name, participant.identity, participant.name)
                    .await;
            }
            "participant_left" => {
                let Some(room) = room else {
                    return;
                };
                let Some(participant) = participant else {
                    return;
                };
                let room_is_empty = self
                    .rooms
                    .list_participants(&room.name)
                    .map(|participants| participants.is_empty())
                    .unwrap_or(false);
                self.runtime
                    .release_departed_participant_jobs(
                        &room.name,
                        &participant.identity,
                        room_is_empty,
                    )
                    .await;
            }
            "room_finished" => {
                let Some(room) = room else {
                    return;
                };
                self.runtime.release_room_jobs(&room.name).await;
            }
            _ => {}
        }
    }

    fn spawn_state_reconciler(&self) {
        let state = self.clone();
        tokio::spawn(async move {
            loop {
                state.reconcile_from_room_store().await;
                tokio::time::sleep(Duration::from_millis(250)).await;
            }
        });
    }

    async fn reconcile_from_room_store(&self) {
        let Ok(rooms) = self.rooms.list_rooms(&[]) else {
            return;
        };

        for room in rooms {
            let room_name = room.name;
            self.try_dispatch_room_job(room_name.clone()).await;

            let Ok(participants) = self.rooms.list_participants(&room_name) else {
                continue;
            };

            for participant in participants {
                let identity = participant.identity;
                let name = participant.name;
                self.try_dispatch_participant_job(
                    room_name.clone(),
                    identity.clone(),
                    name.clone(),
                )
                .await;
                if !participant.tracks.is_empty() {
                    self.try_dispatch_publisher_job(room_name.clone(), identity, name)
                        .await;
                }
            }
        }
    }

    async fn try_dispatch_room_job(&self, room_name: String) {
        {
            let mut dispatched = self.runtime.room_jobs_dispatched.lock().await;
            if dispatched.contains(&room_name) {
                return;
            }
            dispatched.insert(room_name.clone());
        }

        if !self
            .dispatch_jobs_for_room(proto::JobType::JtRoom, room_name.clone(), None)
            .await
        {
            let mut dispatched = self.runtime.room_jobs_dispatched.lock().await;
            dispatched.remove(&room_name);
        }
    }

    async fn try_dispatch_participant_job(
        &self,
        room_name: String,
        identity: String,
        name: String,
    ) {
        let key = (room_name.clone(), identity.clone());
        {
            let mut dispatched = self.runtime.participant_jobs_dispatched.lock().await;
            if dispatched.contains(&key) {
                return;
            }
            dispatched.insert(key.clone());
        }

        if !self
            .dispatch_jobs_for_room(
                proto::JobType::JtParticipant,
                room_name,
                Some((identity, name)),
            )
            .await
        {
            let mut dispatched = self.runtime.participant_jobs_dispatched.lock().await;
            dispatched.remove(&key);
        }
    }

    async fn try_dispatch_publisher_job(&self, room_name: String, identity: String, name: String) {
        let key = (room_name.clone(), identity.clone());
        {
            let mut dispatched = self.runtime.publisher_jobs_dispatched.lock().await;
            if dispatched.contains(&key) {
                return;
            }
            dispatched.insert(key.clone());
        }

        if !self
            .dispatch_jobs_for_room(
                proto::JobType::JtPublisher,
                room_name,
                Some((identity, name)),
            )
            .await
        {
            let mut dispatched = self.runtime.publisher_jobs_dispatched.lock().await;
            dispatched.remove(&key);
        }
    }

    async fn dispatch_jobs_for_room(
        &self,
        job_type: proto::JobType,
        room_name: String,
        participant: Option<(String, String)>,
    ) -> bool {
        let dispatches = self.room_dispatches_for(&room_name);

        let room = proto::Room {
            name: room_name,
            ..Default::default()
        };
        let participant_proto = participant.map(|(identity, name)| proto::ParticipantInfo {
            identity,
            name,
            ..Default::default()
        });

        let mut any_assigned = false;
        for dispatch in dispatches {
            let namespaces = self.namespaces_for_dispatch(job_type, &dispatch).await;
            for namespace in namespaces {
                if self
                    .dispatch_job_for_namespace(
                        job_type,
                        &namespace,
                        &dispatch,
                        &room,
                        participant_proto.as_ref(),
                    )
                    .await
                {
                    any_assigned = true;
                }
            }
        }
        any_assigned
    }

    fn room_dispatches_for(&self, room_name: &str) -> Vec<proto::RoomAgentDispatch> {
        let Ok(room_dispatches) = self.rooms.room_agent_dispatches(room_name) else {
            return vec![proto::RoomAgentDispatch::default()];
        };
        if room_dispatches.is_empty() {
            vec![proto::RoomAgentDispatch::default()]
        } else {
            room_dispatches
        }
    }

    async fn namespaces_for_dispatch(
        &self,
        job_type: proto::JobType,
        dispatch: &proto::RoomAgentDispatch,
    ) -> Vec<String> {
        let workers = self.runtime.workers.lock().await;
        let mut namespaces = workers
            .values()
            .filter(|worker| {
                worker.job_type == job_type as i32
                    && (dispatch.agent_name.is_empty() || worker.agent_name == dispatch.agent_name)
                    && (dispatch.deployment.is_empty() || worker.deployment == dispatch.deployment)
            })
            .map(|worker| worker.namespace.clone())
            .collect::<Vec<_>>();
        namespaces.sort();
        namespaces.dedup();
        namespaces
    }

    #[allow(deprecated)] // Preserve legacy agent job namespace compatibility.
    async fn dispatch_job_for_namespace(
        &self,
        job_type: proto::JobType,
        namespace: &str,
        dispatch: &proto::RoomAgentDispatch,
        room: &proto::Room,
        participant: Option<&proto::ParticipantInfo>,
    ) -> bool {
        let Some(candidate) = self
            .select_dispatch_candidate(
                job_type,
                namespace,
                dispatch.agent_name.as_str(),
                dispatch.deployment.as_str(),
            )
            .await
        else {
            return false;
        };

        let job = proto::Job {
            id: self.next_job_id(),
            dispatch_id: self.next_dispatch_id(),
            r#type: job_type as i32,
            room: Some(room.clone()),
            participant: participant.cloned(),
            namespace: namespace.to_string(),
            agent_name: dispatch.agent_name.clone(),
            metadata: dispatch.metadata.clone(),
            attributes: dispatch.attributes.clone(),
            deployment: dispatch.deployment.clone(),
            ..Default::default()
        };

        let availability_request = proto::ServerMessage {
            message: Some(proto::server_message::Message::Availability(
                proto::AvailabilityRequest {
                    job: Some(job.clone()),
                    resuming: false,
                },
            )),
        };

        let (availability_tx, availability_rx) = oneshot::channel();
        {
            let mut workers = self.runtime.workers.lock().await;
            let Some(worker) = workers.get_mut(&candidate.worker_id) else {
                return false;
            };
            worker
                .pending_availability
                .insert(job.id.clone(), availability_tx);
        }

        if candidate.outbound.send(availability_request).is_err() {
            let mut workers = self.runtime.workers.lock().await;
            if let Some(worker) = workers.get_mut(&candidate.worker_id) {
                worker.pending_availability.remove(&job.id);
            }
            return false;
        }

        let availability = tokio::time::timeout(ASSIGN_JOB_TIMEOUT, availability_rx)
            .await
            .ok()
            .and_then(Result::ok);

        let Some(availability) = availability else {
            return false;
        };

        if availability.terminate || !availability.available {
            return false;
        }

        let assignment_token =
            self.build_assignment_token(&candidate, room, &job.id, &job.agent_name, &availability);

        let assignment = proto::ServerMessage {
            message: Some(proto::server_message::Message::Assignment(
                proto::JobAssignment {
                    job: Some(job),
                    token: assignment_token,
                    ..Default::default()
                },
            )),
        };

        candidate.outbound.send(assignment).is_ok()
    }

    async fn select_dispatch_candidate(
        &self,
        job_type: proto::JobType,
        namespace: &str,
        agent_name: &str,
        deployment: &str,
    ) -> Option<DispatchCandidate> {
        let workers = self.runtime.workers.lock().await;

        let weighted_workers = workers
            .values()
            .filter(|worker| {
                worker.job_type == job_type as i32
                    && worker.namespace == namespace
                    && worker.status == proto::WorkerStatus::WsAvailable as i32
                    && (agent_name.is_empty() || worker.agent_name == agent_name)
                    && (deployment.is_empty() || worker.deployment == deployment)
            })
            .map(|worker| (worker, (1.0_f32 - worker.load).max(0.0)))
            .filter(|(_, weight)| *weight > 0.0)
            .collect::<Vec<_>>();

        let total_weight: f32 = weighted_workers.iter().map(|(_, weight)| *weight).sum();
        if total_weight <= 0.0 {
            return None;
        }

        let now_nanos = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.subsec_nanos())
            .unwrap_or_default();
        let mut sample = (now_nanos as f32 / u32::MAX as f32) * total_weight;

        for (worker, weight) in weighted_workers {
            sample -= weight;
            if sample <= 0.0 {
                return Some(DispatchCandidate {
                    worker_id: worker.id.clone(),
                    api_key: worker.api_key.clone(),

                    permissions: worker.permissions.clone(),
                    outbound: worker.outbound.clone(),
                });
            }
        }

        None
    }

    fn build_assignment_token(
        &self,
        candidate: &DispatchCandidate,
        room: &proto::Room,
        job_id: &str,
        agent_name: &str,
        availability: &proto::AvailabilityResponse,
    ) -> String {
        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as usize)
            .unwrap_or_default();

        let participant_identity = if availability.participant_identity.is_empty() {
            format!("agent-{job_id}")
        } else {
            availability.participant_identity.clone()
        };

        let participant_name = if availability.participant_name.is_empty() {
            participant_identity.clone()
        } else {
            availability.participant_name.clone()
        };

        let mut attributes = availability.participant_attributes.clone();
        attributes.insert(AGENT_NAME_ATTRIBUTE_KEY.to_string(), agent_name.to_string());

        let claims = Claims {
            exp: now.saturating_add(Duration::from_secs(3600 * 6).as_secs() as usize),
            nbf: now.saturating_sub(1),
            iss: candidate.api_key.clone(),
            sub: participant_identity.clone(),
            identity: participant_identity,
            name: participant_name,
            metadata: availability.participant_metadata.clone(),
            attributes,
            video: permissions_to_video_grants(&candidate.permissions, &room.name),
            ..Default::default()
        };

        self.auth
            .issue_token(&candidate.api_key, &claims)
            .unwrap_or_default()
    }

    async fn register_worker(
        &self,
        request: &proto::RegisterWorkerRequest,
        api_key: &str,
        outbound: mpsc::UnboundedSender<proto::ServerMessage>,
    ) -> Option<String> {
        if proto::JobType::try_from(request.r#type).is_err() {
            return None;
        }

        let worker_id = self.next_worker_id();
        let namespace = request.namespace.clone().unwrap_or_default();

        let permissions =
            request
                .allowed_permissions
                .clone()
                .unwrap_or(proto::ParticipantPermission {
                    can_subscribe: true,
                    can_publish: true,
                    can_publish_data: true,
                    can_update_metadata: true,
                    ..Default::default()
                });

        let worker = RegisteredWorker {
            id: worker_id.clone(),
            api_key: api_key.to_string(),
            job_type: request.r#type,
            namespace,
            agent_name: request.agent_name.clone(),
            deployment: request.deployment.clone(),
            permissions,
            status: proto::WorkerStatus::WsAvailable as i32,
            load: 0.0,
            outbound,
            pending_availability: HashMap::new(),
        };

        self.runtime
            .workers
            .lock()
            .await
            .insert(worker_id.clone(), worker);

        Some(worker_id)
    }

    async fn update_worker_status(&self, worker_id: &str, update: &proto::UpdateWorkerStatus) {
        let mut workers = self.runtime.workers.lock().await;
        let Some(worker) = workers.get_mut(worker_id) else {
            return;
        };

        if let Some(status) = update.status {
            worker.status = status;
        }
        worker.load = update.load;
    }

    async fn handle_availability_response(
        &self,
        worker_id: &str,
        response: proto::AvailabilityResponse,
    ) {
        let mut workers = self.runtime.workers.lock().await;
        let Some(worker) = workers.get_mut(worker_id) else {
            return;
        };

        if let Some(pending) = worker.pending_availability.remove(&response.job_id) {
            let _ = pending.send(response);
        }
    }

    async fn unregister_worker(&self, worker_id: &str) {
        let job_type = {
            let mut workers = self.runtime.workers.lock().await;
            let Some(worker) = workers.remove(worker_id) else {
                return;
            };
            if workers
                .values()
                .any(|remaining| remaining.job_type == worker.job_type)
            {
                return;
            }
            proto::JobType::try_from(worker.job_type).ok()
        };

        if let Some(job_type) = job_type {
            self.runtime.release_jobs_for_type(job_type).await;
        }
    }
}

impl RedisWebhookRelay {
    fn publish(&self, payload: Vec<u8>) {
        let redis_url = self.redis_url.clone();
        tokio::task::spawn_blocking(move || {
            let Ok(client) = redis::Client::open(redis_url.as_ref().as_str()) else {
                return;
            };
            let Ok(mut connection) = client.get_connection() else {
                return;
            };
            let _ = redis::cmd("PUBLISH")
                .arg(REDIS_AGENT_WEBHOOK_CHANNEL)
                .arg(payload)
                .query::<i64>(&mut connection);
        });
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[tokio::test]
    async fn departed_participant_releases_its_jobs_and_an_empty_room_releases_its_room_job() {
        let runtime = RuntimeState {
            workers: Mutex::new(HashMap::new()),
            room_jobs_dispatched: Mutex::new(HashSet::from(["room-a".to_string()])),
            participant_jobs_dispatched: Mutex::new(HashSet::from([
                ("room-a".to_string(), "alice".to_string()),
                ("room-a".to_string(), "bob".to_string()),
            ])),
            publisher_jobs_dispatched: Mutex::new(HashSet::from([
                ("room-a".to_string(), "alice".to_string()),
                ("room-a".to_string(), "bob".to_string()),
            ])),
        };

        runtime
            .release_departed_participant_jobs("room-a", "alice", false)
            .await;

        assert!(runtime.room_jobs_dispatched.lock().await.contains("room-a"));
        assert!(
            !runtime
                .participant_jobs_dispatched
                .lock()
                .await
                .contains(&("room-a".to_string(), "alice".to_string()))
        );
        assert!(
            runtime
                .participant_jobs_dispatched
                .lock()
                .await
                .contains(&("room-a".to_string(), "bob".to_string()))
        );
        assert!(
            !runtime
                .publisher_jobs_dispatched
                .lock()
                .await
                .contains(&("room-a".to_string(), "alice".to_string()))
        );

        runtime
            .release_departed_participant_jobs("room-a", "bob", true)
            .await;

        assert!(!runtime.room_jobs_dispatched.lock().await.contains("room-a"));
        assert!(runtime.participant_jobs_dispatched.lock().await.is_empty());
        assert!(runtime.publisher_jobs_dispatched.lock().await.is_empty());
    }

    #[tokio::test]
    async fn releasing_a_worker_type_only_releases_that_type_of_dispatch_marker() {
        let runtime = RuntimeState {
            workers: Mutex::new(HashMap::new()),
            room_jobs_dispatched: Mutex::new(HashSet::from(["room-a".to_string()])),
            participant_jobs_dispatched: Mutex::new(HashSet::from([(
                "room-a".to_string(),
                "alice".to_string(),
            )])),
            publisher_jobs_dispatched: Mutex::new(HashSet::from([(
                "room-a".to_string(),
                "alice".to_string(),
            )])),
        };

        runtime.release_jobs_for_type(proto::JobType::JtRoom).await;

        assert!(runtime.room_jobs_dispatched.lock().await.is_empty());
        assert!(!runtime.participant_jobs_dispatched.lock().await.is_empty());
        assert!(!runtime.publisher_jobs_dispatched.lock().await.is_empty());

        runtime
            .release_jobs_for_type(proto::JobType::JtParticipant)
            .await;

        assert!(runtime.participant_jobs_dispatched.lock().await.is_empty());
        assert!(!runtime.publisher_jobs_dispatched.lock().await.is_empty());
    }
}

#[allow(deprecated)] // Preserve the legacy participant recorder grant.
fn permissions_to_video_grants(
    permissions: &proto::ParticipantPermission,
    room_name: &str,
) -> VideoGrants {
    let can_publish_sources = permissions
        .can_publish_sources
        .iter()
        .filter_map(|source| proto::TrackSource::try_from(*source).ok())
        .map(|source| format!("{source:?}").to_ascii_lowercase())
        .collect();

    VideoGrants {
        room_join: true,
        room: room_name.to_string(),
        can_publish: permissions.can_publish,
        can_subscribe: permissions.can_subscribe,
        can_publish_data: permissions.can_publish_data,
        can_publish_sources,
        can_update_own_metadata: permissions.can_update_metadata,
        hidden: permissions.hidden,
        recorder: permissions.recorder,
        ..Default::default()
    }
}

#[derive(Debug, Deserialize)]
struct AgentQuery {
    access_token: Option<String>,
}

pub fn router(state: AgentState) -> Router {
    Router::new()
        .route("/agent", get(agent_socket))
        .with_state(state)
}

fn validate_agent_access(
    state: &AgentState,
    headers: &HeaderMap,
    query: &AgentQuery,
) -> Result<AuthContext, AuthError> {
    let auth = if let Some(token) = query.access_token.as_deref() {
        state.auth.verify_token(token)?
    } else {
        let header_value = headers
            .get(header::AUTHORIZATION)
            .and_then(|value| value.to_str().ok())
            .ok_or(AuthError::MissingBearer)?;
        state.auth.verify_authorization_header(header_value)?
    };

    auth.ensure_agent_permission()?;
    Ok(auth)
}

async fn agent_socket(
    State(state): State<AgentState>,
    headers: HeaderMap,
    Query(query): Query<AgentQuery>,
    ws: WebSocketUpgrade,
) -> Response {
    let auth = match validate_agent_access(&state, &headers, &query) {
        Ok(auth) => auth,
        Err(_) => return (StatusCode::UNAUTHORIZED, "permission denied").into_response(),
    };

    ws.on_upgrade(move |socket| async move {
        run_agent_socket(socket, state, auth).await;
    })
}

async fn run_agent_socket(socket: WebSocket, state: AgentState, auth: AuthContext) {
    let (mut sender, mut receiver) = socket.split();
    let (outbound_tx, mut outbound_rx) = mpsc::unbounded_channel::<proto::ServerMessage>();

    let outbound_task = tokio::spawn(async move {
        while let Some(message) = outbound_rx.recv().await {
            if sender
                .send(WsMessage::Binary(message.encode_to_vec().into()))
                .await
                .is_err()
            {
                break;
            }
        }
    });

    let mut worker_id: Option<String> = None;
    let mut should_close = false;

    while let Some(next) = receiver.next().await {
        let Ok(message) = next else {
            break;
        };

        match message {
            WsMessage::Binary(bytes) => {
                let Ok(worker_message) = proto::WorkerMessage::decode(bytes.as_ref()) else {
                    should_close = true;
                    break;
                };

                match worker_message.message {
                    Some(proto::worker_message::Message::Register(request)) => {
                        if worker_id.is_some() {
                            should_close = true;
                            break;
                        }

                        let Some(registered_worker_id) = state
                            .register_worker(&request, &auth.api_key, outbound_tx.clone())
                            .await
                        else {
                            should_close = true;
                            break;
                        };

                        let register_response = proto::ServerMessage {
                            message: Some(proto::server_message::Message::Register(
                                proto::RegisterWorkerResponse {
                                    worker_id: registered_worker_id.clone(),
                                    server_info: Some(state.server_info.clone()),
                                },
                            )),
                        };
                        if outbound_tx.send(register_response).is_err() {
                            break;
                        }
                        worker_id = Some(registered_worker_id);
                    }
                    Some(proto::worker_message::Message::UpdateWorker(update)) => {
                        if let Some(worker_id) = worker_id.as_deref() {
                            state.update_worker_status(worker_id, &update).await;
                        } else {
                            should_close = true;
                            break;
                        }
                    }
                    Some(proto::worker_message::Message::Availability(availability)) => {
                        if let Some(worker_id) = worker_id.as_deref() {
                            state
                                .handle_availability_response(worker_id, availability)
                                .await;
                        } else {
                            should_close = true;
                            break;
                        }
                    }
                    Some(proto::worker_message::Message::Ping(ping)) => {
                        let _ = outbound_tx.send(proto::ServerMessage {
                            message: Some(proto::server_message::Message::Pong(
                                proto::WorkerPong {
                                    last_timestamp: ping.timestamp,
                                    timestamp: ping.timestamp,
                                },
                            )),
                        });
                    }
                    Some(proto::worker_message::Message::UpdateJob(_))
                    | Some(proto::worker_message::Message::SimulateJob(_))
                    | Some(proto::worker_message::Message::MigrateJob(_)) => {}
                    None => {
                        should_close = true;
                        break;
                    }
                }
            }
            WsMessage::Close(_) => break,
            WsMessage::Ping(_) | WsMessage::Pong(_) | WsMessage::Text(_) => {}
        }
    }

    if let Some(worker_id) = worker_id.as_deref() {
        state.unregister_worker(worker_id).await;
    }

    if should_close {
        outbound_task.abort();
    }
}
