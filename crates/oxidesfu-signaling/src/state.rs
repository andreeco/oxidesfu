use std::{
    collections::{HashMap, HashSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use livekit_protocol as proto;
use oxidesfu_auth::{AuthContext, Claims, TokenVerifier, VideoGrants};
use oxidesfu_room::{RoomNodeDirectory, RoomStore, RoomStoreError};
use prost::Message;
use tokio::sync::{mpsc, oneshot};

use crate::{
    data::DataChannelMessageStore,
    errors::SignalResult,
    media::{TrackAllocationStore, TrackSettingsStore},
    relay::{
        NonLocalRelayDispatcher, NonLocalRelayRoomServiceIntent, NonLocalRelayRoomServiceResponse,
        NonLocalRelaySignalRequestResponse, NoopNonLocalRelayDispatcher,
    },
    router::{
        DataChannelKind, DataChannelStore, OutboundSignalSender, PeerConnectionStore,
        RtpForwardingStore, session::handle_media_subscription_request,
    },
    signal_request::signal_response_for_request,
    socket::drain_relay_outbound_responses,
    stores::{
        AutoSubscribePreferenceStore, DataTrackStore, DataTrackSubscriptionStore,
        ForwardTrackStore, MediaForwardingStore, MediaSubscriptionStore, MediaTrackCidStore,
        PendingMediaSectionRequestStore, PendingPublisherRemoteTrackStore, PublishPermissionStore,
        SignalConnectionStore, SinglePcOfferMediaKindStore, SubscribePermissionStore,
        SubscriberOfferIdStore, SubscriberOfferNegotiationStore,
    },
};

const DEFAULT_RPC_RESPONSE_TIMEOUT_MS: u32 = 10_000;
const MAX_RPC_PAYLOAD_BYTES: usize = 15 * 1024;
const MAX_RPC_METHOD_BYTES: usize = 64;
const DEFAULT_PARTICIPANT_DATA_BLOB_MAX_KEY_LENGTH: usize = 256;

static NEXT_SERVICE_RPC_REQUEST_ID: AtomicU64 = AtomicU64::new(1);

#[derive(Debug)]
enum ServiceRpcResponse {
    Payload(String),
    Error(proto::RpcError),
    CompressedPayload,
}

/// Shared state for signalling handlers.
type WebhookEventHandler = Arc<dyn Fn(proto::WebhookEvent) + Send + Sync>;

/// Produces participant-specific ICE servers from a LiveKit participant SID.
pub type IceServerProvider = Arc<dyn Fn(&str) -> Vec<proto::IceServer> + Send + Sync>;

#[derive(Clone)]
pub struct SignalState {
    pub rooms: RoomStore,
    pub auth: TokenVerifier,
    pub(crate) room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    pub(crate) local_room_node_id: Option<String>,
    pub(crate) reject_non_local_room_placement: bool,
    pub(crate) non_local_relay_dispatcher: Arc<dyn NonLocalRelayDispatcher>,
    pub(crate) updates: crate::router::ParticipantUpdateHub,
    pub(crate) peer_connections: PeerConnectionStore,
    pub(crate) data_messages: DataChannelMessageStore,
    pub(crate) data_channels: DataChannelStore,
    pub(crate) data_tracks: DataTrackStore,
    pub(crate) data_track_subscriptions: DataTrackSubscriptionStore,
    pub(crate) publish_permissions: PublishPermissionStore,
    pub(crate) subscribe_permissions: SubscribePermissionStore,
    pub(crate) auto_subscribe_preferences: AutoSubscribePreferenceStore,
    pub(crate) track_settings: TrackSettingsStore,
    pub(crate) track_allocations: TrackAllocationStore,
    pub(crate) media_subscription_limit_audio: Option<usize>,
    pub(crate) media_subscription_limit_video: Option<usize>,
    pub(crate) media_forwarding: MediaForwardingStore,
    pub(crate) pending_media_section_requests: PendingMediaSectionRequestStore,
    pub(crate) media_subscriptions: MediaSubscriptionStore,
    pub(crate) media_track_cids: MediaTrackCidStore,
    pub(crate) pending_remote_tracks: PendingPublisherRemoteTrackStore,
    pub(crate) forward_tracks: ForwardTrackStore,
    pub(crate) rtp_forwarding: RtpForwardingStore,
    pub(crate) signal_connections: SignalConnectionStore,
    pub(crate) subscriber_offer_ids: SubscriberOfferIdStore,
    pub(crate) subscriber_offer_negotiations: SubscriberOfferNegotiationStore,
    subscriber_offer_mid_track_ids:
        Arc<Mutex<HashMap<(String, String, u32), HashMap<String, String>>>>,
    pub(crate) single_pc_offer_media_kinds: SinglePcOfferMediaKindStore,
    pub(crate) ice_servers: Vec<proto::IceServer>,
    ice_server_provider: Option<IceServerProvider>,
    pub(crate) rtc_transport: oxidesfu_rtc::RtcTransportConfig,
    room_auto_create_on_join: bool,
    datachannel_slow_threshold_bytes: Option<u32>,
    reconnect_participant_retention_grace: Duration,
    candidate_protocol_preferences: Arc<Mutex<HashMap<(String, String), i32>>>,
    service_rpc_pending: Arc<Mutex<HashMap<String, oneshot::Sender<ServiceRpcResponse>>>>,
    webhook_event_handler: Option<WebhookEventHandler>,
    participant_auth_contexts: Arc<Mutex<HashMap<(String, String), AuthContext>>>,
    participant_client_infos: Arc<Mutex<HashMap<(String, String), proto::ClientInfo>>>,
    participant_subscribe_video_mime_types: Arc<Mutex<HashMap<(String, String), HashSet<String>>>>,
    participant_subscriber_primary: Arc<Mutex<HashMap<(String, String), bool>>>,
    publisher_subscription_active_pairs: Arc<Mutex<HashSet<(String, String, String)>>>,
    participant_data_blobs: Arc<Mutex<HashMap<(String, String, Vec<u8>), Vec<u8>>>>,
    test_support_available_outgoing_bitrate_bps: Arc<Mutex<HashMap<(String, String), u64>>>,
    participant_data_blob_enabled: bool,
    participant_data_blob_max_key_length: usize,
}

impl SignalState {
    fn default_ice_servers() -> Vec<proto::IceServer> {
        vec![proto::IceServer {
            urls: vec!["stun:stun.l.google.com:19302".to_string()],
            ..Default::default()
        }]
    }

    /// Creates signalling state from shared room/auth components.
    pub fn new(rooms: RoomStore, auth: TokenVerifier) -> Self {
        Self::with_data_channels(rooms, auth, DataChannelStore::default())
    }

    /// Creates signalling state with an externally shared data-channel store.
    pub fn with_data_channels(
        rooms: RoomStore,
        auth: TokenVerifier,
        data_channels: DataChannelStore,
    ) -> Self {
        Self::with_data_channels_and_room_nodes(rooms, auth, data_channels, None)
    }

    /// Creates signalling state with shared data channels and optional room-node directory.
    pub fn with_data_channels_and_room_nodes(
        rooms: RoomStore,
        auth: TokenVerifier,
        data_channels: DataChannelStore,
        room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
    ) -> Self {
        Self::with_data_channels_room_nodes_and_placement(
            rooms,
            auth,
            data_channels,
            room_nodes,
            None,
            false,
        )
    }

    /// Creates signalling state with explicit room-node placement behavior controls.
    pub fn with_data_channels_room_nodes_and_placement(
        rooms: RoomStore,
        auth: TokenVerifier,
        data_channels: DataChannelStore,
        room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
        local_room_node_id: Option<String>,
        reject_non_local_room_placement: bool,
    ) -> Self {
        Self::with_data_channels_room_nodes_placement_and_relay_dispatcher(
            rooms,
            auth,
            data_channels,
            room_nodes,
            local_room_node_id,
            reject_non_local_room_placement,
            Arc::new(NoopNonLocalRelayDispatcher),
        )
    }

    /// Creates signalling state with explicit placement controls and injected non-local relay dispatcher.
    pub fn with_data_channels_room_nodes_placement_and_relay_dispatcher(
        rooms: RoomStore,
        auth: TokenVerifier,
        data_channels: DataChannelStore,
        room_nodes: Option<Arc<dyn RoomNodeDirectory>>,
        local_room_node_id: Option<String>,
        reject_non_local_room_placement: bool,
        non_local_relay_dispatcher: Arc<dyn NonLocalRelayDispatcher>,
    ) -> Self {
        Self {
            rooms,
            auth,
            room_nodes,
            local_room_node_id,
            reject_non_local_room_placement,
            non_local_relay_dispatcher,
            updates: crate::router::ParticipantUpdateHub::default(),
            peer_connections: PeerConnectionStore::default(),
            data_messages: DataChannelMessageStore::default(),
            data_channels,
            data_tracks: DataTrackStore::default(),
            data_track_subscriptions: DataTrackSubscriptionStore::default(),
            publish_permissions: PublishPermissionStore::default(),
            subscribe_permissions: SubscribePermissionStore::default(),
            auto_subscribe_preferences: AutoSubscribePreferenceStore::default(),
            track_settings: TrackSettingsStore::default(),
            track_allocations: TrackAllocationStore::default(),
            media_subscription_limit_audio: None,
            media_subscription_limit_video: None,
            media_forwarding: MediaForwardingStore::default(),
            pending_media_section_requests: PendingMediaSectionRequestStore::default(),
            media_subscriptions: MediaSubscriptionStore::default(),
            media_track_cids: MediaTrackCidStore::default(),
            pending_remote_tracks: PendingPublisherRemoteTrackStore::default(),
            forward_tracks: ForwardTrackStore::default(),
            rtp_forwarding: RtpForwardingStore::default(),
            signal_connections: SignalConnectionStore::default(),
            subscriber_offer_ids: SubscriberOfferIdStore::default(),
            subscriber_offer_negotiations: SubscriberOfferNegotiationStore::default(),
            subscriber_offer_mid_track_ids: Arc::new(Mutex::new(HashMap::new())),
            single_pc_offer_media_kinds: SinglePcOfferMediaKindStore::default(),
            ice_servers: Self::default_ice_servers(),
            ice_server_provider: None,
            rtc_transport: oxidesfu_rtc::RtcTransportConfig::default(),
            room_auto_create_on_join: true,
            datachannel_slow_threshold_bytes: None,
            reconnect_participant_retention_grace: Duration::from_secs(15),
            candidate_protocol_preferences: Arc::new(Mutex::new(HashMap::new())),
            service_rpc_pending: Arc::new(Mutex::new(HashMap::new())),
            webhook_event_handler: None,
            participant_auth_contexts: Arc::new(Mutex::new(HashMap::new())),
            participant_client_infos: Arc::new(Mutex::new(HashMap::new())),
            participant_subscribe_video_mime_types: Arc::new(Mutex::new(HashMap::new())),
            participant_subscriber_primary: Arc::new(Mutex::new(HashMap::new())),
            publisher_subscription_active_pairs: Arc::new(Mutex::new(HashSet::new())),
            participant_data_blobs: Arc::new(Mutex::new(HashMap::new())),
            test_support_available_outgoing_bitrate_bps: Arc::new(Mutex::new(HashMap::new())),
            participant_data_blob_enabled: true,
            participant_data_blob_max_key_length: DEFAULT_PARTICIPANT_DATA_BLOB_MAX_KEY_LENGTH,
        }
    }

    /// Overrides advertised ICE servers used in join/reconnect responses.
    pub fn with_ice_servers(mut self, ice_servers: Vec<proto::IceServer>) -> Self {
        self.ice_servers = ice_servers;
        self
    }

    /// Configures participant-specific ICE servers for join and reconnect responses.
    ///
    /// The provider receives the participant SID, never the participant identity. When no
    /// provider is configured, [`Self::with_ice_servers`] supplies the static fallback.
    pub fn with_ice_server_provider<F>(mut self, provider: F) -> Self
    where
        F: Fn(&str) -> Vec<proto::IceServer> + Send + Sync + 'static,
    {
        self.ice_server_provider = Some(Arc::new(provider));
        self
    }

    pub(crate) fn ice_servers(&self, participant_sid: &str) -> Vec<proto::IceServer> {
        self.ice_server_provider
            .as_ref()
            .map(|provider| provider(participant_sid))
            .unwrap_or_else(|| self.ice_servers.clone())
    }

    pub fn with_rtc_transport_config(
        mut self,
        rtc_transport: oxidesfu_rtc::RtcTransportConfig,
    ) -> Self {
        self.rtc_transport = rtc_transport;
        self
    }

    pub(crate) fn rtc_transport_config(&self) -> oxidesfu_rtc::RtcTransportConfig {
        self.rtc_transport.clone()
    }

    pub fn with_room_auto_create(mut self, room_auto_create_on_join: bool) -> Self {
        self.room_auto_create_on_join = room_auto_create_on_join;
        self
    }

    pub(crate) fn room_auto_create_on_join(&self) -> bool {
        self.room_auto_create_on_join
    }

    pub fn with_media_subscription_limits(
        mut self,
        audio_limit: Option<usize>,
        video_limit: Option<usize>,
    ) -> Self {
        self.media_subscription_limit_audio = audio_limit;
        self.media_subscription_limit_video = video_limit;
        self
    }

    pub fn with_datachannel_slow_threshold_bytes(mut self, threshold: Option<u32>) -> Self {
        self.datachannel_slow_threshold_bytes = threshold;
        self
    }

    pub(crate) fn datachannel_slow_threshold_bytes(&self) -> Option<u32> {
        self.datachannel_slow_threshold_bytes
    }

    /// Sets or clears a deterministic receiver bandwidth source for test support only.
    ///
    /// A set value applies only to the given room and subscriber identity. Clearing it restores
    /// the production allocator's candidate-pair RTC statistics source.
    #[doc(hidden)]
    pub fn set_test_support_available_outgoing_bitrate_bps(
        &self,
        room_name: &str,
        subscriber_identity: &str,
        bitrate_bps: Option<u64>,
    ) {
        if let Ok(mut overrides) = self.test_support_available_outgoing_bitrate_bps.lock() {
            let key = (room_name.to_string(), subscriber_identity.to_string());
            if let Some(bitrate_bps) = bitrate_bps {
                overrides.insert(key, bitrate_bps);
            } else {
                overrides.remove(&key);
            }
        }
    }

    pub(crate) fn test_support_available_outgoing_bitrate_bps(
        &self,
        room_name: &str,
        subscriber_identity: &str,
    ) -> Option<u64> {
        self.test_support_available_outgoing_bitrate_bps
            .lock()
            .ok()
            .and_then(|overrides| {
                overrides
                    .get(&(room_name.to_string(), subscriber_identity.to_string()))
                    .copied()
            })
    }

    pub fn with_participant_data_blob_enabled(mut self, enabled: bool) -> Self {
        self.participant_data_blob_enabled = enabled;
        self
    }

    pub(crate) fn participant_data_blob_enabled(&self) -> bool {
        self.participant_data_blob_enabled
    }

    pub fn with_participant_data_blob_max_key_length(mut self, max_key_length: usize) -> Self {
        self.participant_data_blob_max_key_length = max_key_length;
        self
    }

    pub(crate) fn participant_data_blob_max_key_length(&self) -> usize {
        self.participant_data_blob_max_key_length
    }

    /// Resolves non-local room service relay target for an existing room mapping.
    pub fn non_local_room_service_target_for_room(
        &self,
        room: &str,
    ) -> Result<Option<String>, String> {
        let (Some(room_nodes), Some(local_room_node_id)) =
            (self.room_nodes.as_ref(), self.local_room_node_id.as_ref())
        else {
            return Ok(None);
        };

        match room_nodes.get_node_for_room(room) {
            Ok(selected) => {
                if selected.id == *local_room_node_id {
                    Ok(None)
                } else {
                    Ok(Some(selected.id))
                }
            }
            Err(oxidesfu_room::RoomNodeRegistryError::NodeNotFound) => Ok(None),
            Err(err) => Err(err.to_string()),
        }
    }

    /// Dispatches a non-local room service relay intent through the configured dispatcher.
    pub fn dispatch_non_local_room_service(
        &self,
        intent: NonLocalRelayRoomServiceIntent,
    ) -> Result<Option<NonLocalRelayRoomServiceResponse>, String> {
        self.non_local_relay_dispatcher
            .dispatch_non_local_room_service(intent)
    }

    /// Lists registered room-node IDs known to this signalling instance.
    pub fn list_registered_room_node_ids(&self) -> Result<Vec<String>, String> {
        let Some(room_nodes) = self.room_nodes.as_ref() else {
            return Ok(Vec::new());
        };

        room_nodes
            .list_nodes()
            .map(|nodes| nodes.into_iter().map(|node| node.id).collect())
            .map_err(|err| err.to_string())
    }

    /// Returns the configured local room-node ID, when placement mode is enabled.
    pub fn local_room_node_id(&self) -> Option<&str> {
        self.local_room_node_id.as_deref()
    }

    pub fn with_reconnect_participant_retention_grace(mut self, grace: Duration) -> Self {
        self.reconnect_participant_retention_grace = grace;
        self
    }

    pub fn with_webhook_event_handler(
        mut self,
        handler: Option<Arc<dyn Fn(proto::WebhookEvent) + Send + Sync>>,
    ) -> Self {
        self.webhook_event_handler = handler;
        self
    }

    pub fn with_additional_webhook_event_handler(
        mut self,
        handler: Arc<dyn Fn(proto::WebhookEvent) + Send + Sync>,
    ) -> Self {
        self.webhook_event_handler = match self.webhook_event_handler.take() {
            Some(existing) => {
                let chained_existing = existing.clone();
                let chained_added = handler.clone();
                Some(Arc::new(move |event: proto::WebhookEvent| {
                    chained_existing(event.clone());
                    chained_added(event);
                }))
            }
            None => Some(handler),
        };
        self
    }

    pub fn emit_webhook_event(&self, event: proto::WebhookEvent) {
        if let Some(handler) = self.webhook_event_handler.as_ref() {
            handler(event);
        }
    }

    pub(crate) fn clear_publisher_subscription_active(
        &self,
        room_name: &str,
        publisher_identity: &str,
        subscriber_identity: &str,
    ) {
        if let Ok(mut pairs) = self.publisher_subscription_active_pairs.lock() {
            pairs.remove(&(
                room_name.to_string(),
                publisher_identity.to_string(),
                subscriber_identity.to_string(),
            ));
        }
    }

    pub(crate) fn publisher_subscription_active_pairs(
        &self,
    ) -> Arc<Mutex<HashSet<(String, String, String)>>> {
        self.publisher_subscription_active_pairs.clone()
    }

    pub(crate) fn clear_publisher_subscription_active_for_participant(
        &self,
        room_name: &str,
        identity: &str,
    ) {
        if let Ok(mut pairs) = self.publisher_subscription_active_pairs.lock() {
            pairs.retain(
                |(candidate_room, publisher_identity, subscriber_identity)| {
                    candidate_room != room_name
                        || (publisher_identity != identity && subscriber_identity != identity)
                },
            );
        }
    }

    pub fn remember_participant_auth_context(
        &self,
        room_name: &str,
        identity: &str,
        auth: &AuthContext,
    ) {
        if let Ok(mut contexts) = self.participant_auth_contexts.lock() {
            tracing::debug!(
                room = room_name,
                identity,
                "remembering_participant_auth_context"
            );
            contexts.insert((room_name.to_string(), identity.to_string()), auth.clone());
        }
    }

    pub(crate) fn forget_participant_auth_context(&self, room_name: &str, identity: &str) {
        if let Ok(mut contexts) = self.participant_auth_contexts.lock() {
            tracing::debug!(
                room = room_name,
                identity,
                "forgetting_participant_auth_context"
            );
            contexts.remove(&(room_name.to_string(), identity.to_string()));
        }
    }

    pub(crate) fn remember_participant_client_info(
        &self,
        room_name: &str,
        identity: &str,
        client_info: Option<proto::ClientInfo>,
    ) {
        if let Ok(mut infos) = self.participant_client_infos.lock() {
            let key = (room_name.to_string(), identity.to_string());
            if let Some(client_info) = client_info {
                infos.insert(key, client_info);
            } else {
                infos.remove(&key);
            }
        }
    }

    pub(crate) fn participant_client_info(
        &self,
        room_name: &str,
        identity: &str,
    ) -> Option<proto::ClientInfo> {
        self.participant_client_infos.lock().ok().and_then(|infos| {
            infos
                .get(&(room_name.to_string(), identity.to_string()))
                .cloned()
        })
    }

    pub(crate) fn forget_participant_client_info(&self, room_name: &str, identity: &str) {
        if let Ok(mut infos) = self.participant_client_infos.lock() {
            infos.remove(&(room_name.to_string(), identity.to_string()));
        }
    }

    pub(crate) fn remember_participant_subscribe_video_mime_types(
        &self,
        room_name: &str,
        identity: &str,
        mime_types: &HashSet<String>,
    ) {
        let Ok(mut codecs) = self.participant_subscribe_video_mime_types.lock() else {
            return;
        };
        let key = (room_name.to_string(), identity.to_string());
        if mime_types.is_empty() {
            codecs.remove(&key);
            return;
        }
        codecs.insert(
            key,
            mime_types
                .iter()
                .map(|mime| mime.trim().to_ascii_lowercase())
                .filter(|mime| !mime.is_empty())
                .collect(),
        );
    }

    pub(crate) fn merge_participant_subscribe_video_mime_types(
        &self,
        room_name: &str,
        identity: &str,
        mime_types: &HashSet<String>,
    ) {
        let normalized = mime_types
            .iter()
            .map(|mime| mime.trim().to_ascii_lowercase())
            .filter(|mime| !mime.is_empty())
            .collect::<HashSet<_>>();

        if normalized.is_empty() {
            return;
        }

        let Ok(mut codecs) = self.participant_subscribe_video_mime_types.lock() else {
            return;
        };
        let key = (room_name.to_string(), identity.to_string());
        codecs
            .entry(key)
            .and_modify(|known| {
                known.extend(normalized.iter().cloned());
            })
            .or_insert(normalized);
    }

    pub(crate) fn participant_supports_video_mime_type(
        &self,
        room_name: &str,
        identity: &str,
        mime_type: &str,
    ) -> bool {
        let normalized = mime_type.trim().to_ascii_lowercase();
        if normalized.is_empty() {
            return true;
        }

        let Ok(codecs) = self.participant_subscribe_video_mime_types.lock() else {
            return true;
        };

        match codecs.get(&(room_name.to_string(), identity.to_string())) {
            Some(supported_mime_types) => supported_mime_types.contains(&normalized),
            None => true,
        }
    }

    pub(crate) fn forget_participant_subscribe_video_mime_types(
        &self,
        room_name: &str,
        identity: &str,
    ) {
        if let Ok(mut codecs) = self.participant_subscribe_video_mime_types.lock() {
            codecs.remove(&(room_name.to_string(), identity.to_string()));
        }
    }

    /// Records whether a participant uses the v0 server-offered subscriber transport.
    pub fn remember_participant_subscriber_primary(
        &self,
        room_name: &str,
        identity: &str,
        subscriber_primary: bool,
    ) {
        if let Ok(mut topologies) = self.participant_subscriber_primary.lock() {
            topologies.insert(
                (room_name.to_string(), identity.to_string()),
                subscriber_primary,
            );
        }
    }

    pub(crate) fn participant_uses_subscriber_primary(
        &self,
        room_name: &str,
        identity: &str,
    ) -> bool {
        self.participant_subscriber_primary
            .lock()
            .ok()
            .and_then(|topologies| {
                topologies
                    .get(&(room_name.to_string(), identity.to_string()))
                    .copied()
            })
            .unwrap_or(false)
    }

    pub(crate) fn forget_participant_subscriber_primary(&self, room_name: &str, identity: &str) {
        if let Ok(mut topologies) = self.participant_subscriber_primary.lock() {
            topologies.remove(&(room_name.to_string(), identity.to_string()));
        }
    }

    pub(crate) fn remember_subscriber_offer_mid_track_ids(
        &self,
        room_name: &str,
        identity: &str,
        offer_id: u32,
        mid_track_ids: HashMap<String, String>,
    ) {
        let Ok(mut mappings) = self.subscriber_offer_mid_track_ids.lock() else {
            return;
        };
        let key = (room_name.to_string(), identity.to_string(), offer_id);
        if mid_track_ids.is_empty() {
            mappings.remove(&key);
            return;
        }
        mappings.insert(key, mid_track_ids);
    }

    pub(crate) fn subscriber_offer_mid_track_ids(
        &self,
        room_name: &str,
        identity: &str,
        offer_id: u32,
    ) -> HashMap<String, String> {
        self.subscriber_offer_mid_track_ids
            .lock()
            .ok()
            .and_then(|mappings| {
                mappings
                    .get(&(room_name.to_string(), identity.to_string(), offer_id))
                    .cloned()
            })
            .unwrap_or_default()
    }

    pub(crate) fn forget_subscriber_offer_mid_track_ids(&self, room_name: &str, identity: &str) {
        let Ok(mut mappings) = self.subscriber_offer_mid_track_ids.lock() else {
            return;
        };
        mappings.retain(|(stored_room, stored_identity, _), _| {
            stored_room != room_name || stored_identity != identity
        });
    }

    pub(crate) fn store_participant_data_blob(
        &self,
        room_name: &str,
        identity: &str,
        key_bytes: Vec<u8>,
        encoded_blob: Vec<u8>,
    ) {
        if let Ok(mut blobs) = self.participant_data_blobs.lock() {
            blobs.insert(
                (room_name.to_string(), identity.to_string(), key_bytes),
                encoded_blob,
            );
        }
    }

    pub(crate) fn participant_data_blob(
        &self,
        room_name: &str,
        identity: &str,
        key_bytes: &[u8],
    ) -> Option<Vec<u8>> {
        self.participant_data_blobs.lock().ok().and_then(|blobs| {
            blobs
                .get(&(
                    room_name.to_string(),
                    identity.to_string(),
                    key_bytes.to_vec(),
                ))
                .cloned()
        })
    }

    pub(crate) fn participant_data_blob_entries(
        &self,
        room_name: &str,
        identity: &str,
    ) -> Vec<(Vec<u8>, Vec<u8>)> {
        self.participant_data_blobs
            .lock()
            .ok()
            .map(|blobs| {
                blobs
                    .iter()
                    .filter_map(|((blob_room, blob_identity, blob_key), blob_value)| {
                        if blob_room == room_name && blob_identity == identity {
                            Some((blob_key.clone(), blob_value.clone()))
                        } else {
                            None
                        }
                    })
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default()
    }

    pub(crate) fn delete_participant_data_blob(
        &self,
        room_name: &str,
        identity: &str,
        key_bytes: &[u8],
    ) {
        if let Ok(mut blobs) = self.participant_data_blobs.lock() {
            blobs.remove(&(
                room_name.to_string(),
                identity.to_string(),
                key_bytes.to_vec(),
            ));
        }
    }

    pub(crate) fn forget_participant_data_blobs(&self, room_name: &str, identity: &str) {
        if let Ok(mut blobs) = self.participant_data_blobs.lock() {
            blobs.retain(|(blob_room, blob_identity, _), _| {
                blob_room != room_name || blob_identity != identity
            });
        }
    }

    pub(crate) fn maybe_issue_refresh_token(
        &self,
        room_name: &str,
        participant: &proto::ParticipantInfo,
    ) -> Option<String> {
        let auth = self
            .participant_auth_contexts
            .lock()
            .ok()
            .and_then(|contexts| {
                contexts
                    .get(&(room_name.to_string(), participant.identity.clone()))
                    .cloned()
            });

        let Some(auth) = auth else {
            tracing::debug!(
                room = room_name,
                identity = %participant.identity,
                "refresh_token_skipped_missing_participant_auth_context"
            );
            return None;
        };

        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .ok()?
            .as_secs() as usize;

        let permission = participant.permission.clone().unwrap_or_default();
        let can_publish_sources = permission
            .can_publish_sources
            .iter()
            .map(|source| {
                proto::TrackSource::try_from(*source)
                    .unwrap_or(proto::TrackSource::Unknown)
                    .as_str_name()
                    .to_ascii_lowercase()
            })
            .collect::<Vec<_>>();

        let claims = Claims {
            exp: now + 60,
            nbf: now.saturating_sub(1),
            iss: auth.api_key.clone(),
            sub: participant.identity.clone(),
            identity: participant.identity.clone(),
            name: participant.name.clone(),
            kind: auth.claims.kind.clone(),
            kind_details: auth.claims.kind_details.clone(),
            video: VideoGrants {
                room_join: true,
                room: room_name.to_string(),
                destination_room: auth.claims.video.destination_room.clone(),
                can_publish: permission.can_publish,
                can_subscribe: permission.can_subscribe,
                can_publish_data: permission.can_publish_data,
                can_publish_sources,
                can_update_own_metadata: permission.can_update_metadata,
                hidden: permission.hidden,
                ..Default::default()
            },
            metadata: participant.metadata.clone(),
            attributes: participant.attributes.clone(),
            room_config: auth.claims.room_config.clone(),
            ..Default::default()
        };

        self.auth.issue_token(&auth.api_key, &claims).ok()
    }

    pub(crate) fn reconnect_participant_retention_grace(&self) -> Duration {
        self.reconnect_participant_retention_grace
    }

    pub(crate) fn set_auto_subscribe_preference(
        &self,
        room_name: &str,
        identity: &str,
        enabled: bool,
    ) {
        self.auto_subscribe_preferences
            .set_auto_subscribe(room_name, identity, enabled);
    }

    pub(crate) fn auto_subscribe_enabled(&self, room_name: &str, identity: &str) -> bool {
        self.auto_subscribe_preferences
            .auto_subscribe_enabled(room_name, identity)
    }

    pub(crate) fn set_auto_subscribe_data_track_preference(
        &self,
        room_name: &str,
        identity: &str,
        enabled: bool,
    ) {
        self.auto_subscribe_preferences
            .set_auto_subscribe_data_track(room_name, identity, enabled);
    }

    pub(crate) fn auto_subscribe_data_track_enabled(
        &self,
        room_name: &str,
        identity: &str,
    ) -> bool {
        self.auto_subscribe_preferences
            .auto_subscribe_data_track_enabled(room_name, identity)
    }

    pub(crate) fn set_candidate_protocol_preference(
        &self,
        room_name: &str,
        identity: &str,
        protocol: i32,
    ) {
        if let Ok(mut preferences) = self.candidate_protocol_preferences.lock() {
            preferences.insert((room_name.to_string(), identity.to_string()), protocol);
        }
    }

    pub(crate) fn candidate_protocol_preference(
        &self,
        room_name: &str,
        identity: &str,
    ) -> Option<i32> {
        self.candidate_protocol_preferences
            .lock()
            .ok()
            .and_then(|preferences| {
                preferences
                    .get(&(room_name.to_string(), identity.to_string()))
                    .copied()
            })
    }

    /// Performs a RoomService-style RPC request against a connected participant.
    pub async fn perform_rpc_from_service(
        &self,
        room_name: &str,
        destination_identity: &str,
        method: &str,
        payload: &str,
        response_timeout_ms: u32,
    ) -> Result<proto::PerformRpcResponse, RoomStoreError> {
        self.rooms
            .get_participant(room_name, destination_identity)?;

        if method.as_bytes().len() > MAX_RPC_METHOD_BYTES {
            return Err(RoomStoreError::InvalidArgument(
                "rpc method must be at most 64 bytes".to_string(),
            ));
        }
        if payload.as_bytes().len() > MAX_RPC_PAYLOAD_BYTES {
            return Err(RoomStoreError::InvalidArgument(
                "rpc payload must be at most 15KiB".to_string(),
            ));
        }

        let Some(destination_channel) = self.data_channels.get_with_kind(
            room_name,
            destination_identity,
            DataChannelKind::Reliable,
        ) else {
            return Err(RoomStoreError::ParticipantNotFound);
        };

        let request_id = format!(
            "rpc-{:016x}",
            NEXT_SERVICE_RPC_REQUEST_ID.fetch_add(1, Ordering::Relaxed)
        );
        let timeout_ms = if response_timeout_ms == 0 {
            DEFAULT_RPC_RESPONSE_TIMEOUT_MS
        } else {
            response_timeout_ms
        };

        let (response_tx, response_rx) = oneshot::channel();
        self.service_rpc_pending
            .lock()
            .map_err(|_| RoomStoreError::LockPoisoned)?
            .insert(request_id.clone(), response_tx);

        let request_packet = proto::DataPacket {
            kind: proto::data_packet::Kind::Reliable as i32,
            participant_identity: request_id.clone(),
            value: Some(proto::data_packet::Value::RpcRequest(proto::RpcRequest {
                id: request_id.clone(),
                method: method.to_string(),
                payload: payload.to_string(),
                response_timeout_ms: timeout_ms,
                version: 1,
                ..Default::default()
            })),
            ..Default::default()
        };

        if let Err(err) = destination_channel
            .send_bytes(&request_packet.encode_to_vec())
            .await
        {
            if let Ok(mut pending) = self.service_rpc_pending.lock() {
                pending.remove(&request_id);
            }
            return Err(RoomStoreError::InvalidArgument(format!(
                "failed to send rpc request: {err}"
            )));
        }

        match tokio::time::timeout(Duration::from_millis(timeout_ms as u64), response_rx).await {
            Ok(Ok(ServiceRpcResponse::Payload(payload))) => {
                Ok(proto::PerformRpcResponse { payload })
            }
            Ok(Ok(ServiceRpcResponse::Error(err))) => {
                Err(RoomStoreError::InvalidArgument(if err.message.is_empty() {
                    format!("rpc error code {}", err.code)
                } else {
                    format!("rpc error {}: {}", err.code, err.message)
                }))
            }
            Ok(Ok(ServiceRpcResponse::CompressedPayload)) => Err(RoomStoreError::InvalidArgument(
                "compressed rpc response payload is not supported".to_string(),
            )),
            Ok(Err(_)) => Err(RoomStoreError::ParticipantNotFound),
            Err(_) => {
                if let Ok(mut pending) = self.service_rpc_pending.lock() {
                    pending.remove(&request_id);
                }
                Err(RoomStoreError::InvalidArgument(
                    "rpc response timeout".to_string(),
                ))
            }
        }
    }

    pub(crate) fn consume_service_rpc_data_packet(&self, packet: &proto::DataPacket) -> bool {
        match packet.value.as_ref() {
            Some(proto::data_packet::Value::RpcAck(ack)) => self
                .service_rpc_pending
                .lock()
                .ok()
                .is_some_and(|pending| pending.contains_key(&ack.request_id)),
            Some(proto::data_packet::Value::RpcResponse(response)) => {
                let response_value = match response.value.as_ref() {
                    Some(proto::rpc_response::Value::Payload(payload)) => {
                        ServiceRpcResponse::Payload(payload.clone())
                    }
                    Some(proto::rpc_response::Value::Error(err)) => {
                        ServiceRpcResponse::Error(err.clone())
                    }
                    Some(proto::rpc_response::Value::CompressedPayload(_)) => {
                        ServiceRpcResponse::CompressedPayload
                    }
                    None => ServiceRpcResponse::Payload(String::new()),
                };

                let Some(response_tx) = self
                    .service_rpc_pending
                    .lock()
                    .ok()
                    .and_then(|mut pending| pending.remove(&response.request_id))
                else {
                    return false;
                };

                let _ = response_tx.send(response_value);
                true
            }
            _ => false,
        }
    }

    /// Handles a signal request on behalf of a non-local relay worker.
    pub async fn handle_relayed_signal_request(
        &self,
        room_name: &str,
        identity: &str,
        request: proto::SignalRequest,
    ) -> SignalResult<Option<proto::SignalResponse>> {
        let (outbound_tx, _outbound_rx) = mpsc::unbounded_channel();
        signal_response_for_request(request, self, room_name, identity, &outbound_tx).await
    }

    /// Applies a Twirp-style media subscription update to the active signalling runtime.
    pub async fn apply_twirp_update_subscriptions(
        &self,
        room_name: &str,
        identity: &str,
        track_sids: &[String],
        participant_tracks: &[proto::ParticipantTracks],
        subscribe: bool,
    ) {
        let request = proto::UpdateSubscription {
            track_sids: track_sids.to_vec(),
            participant_tracks: participant_tracks.to_vec(),
            subscribe,
        };
        handle_media_subscription_request(self, room_name, identity, request, true).await;
    }

    /// Sends a RoomService `SendData` payload to data-channel subscribers in a room.
    #[allow(deprecated)]
    pub async fn send_data_from_service(
        &self,
        request: &proto::SendDataRequest,
    ) -> Result<(), String> {
        self.rooms
            .ensure_room_exists(&request.room)
            .map_err(|err| err.to_string())?;

        let has_explicit_destinations =
            !request.destination_identities.is_empty() || !request.destination_sids.is_empty();

        let resolved_destination_identities = if request.destination_sids.is_empty() {
            request.destination_identities.clone()
        } else {
            let participants = self
                .rooms
                .list_participants(&request.room)
                .map_err(|err| err.to_string())?;
            let identities_by_sid = participants
                .into_iter()
                .map(|participant| (participant.sid, participant.identity))
                .collect::<HashMap<_, _>>();

            let mut visited = std::collections::HashSet::new();
            let mut resolved = Vec::new();
            for identity in &request.destination_identities {
                if visited.insert(identity.clone()) {
                    resolved.push(identity.clone());
                }
            }
            for sid in &request.destination_sids {
                if let Some(identity) = identities_by_sid.get(sid)
                    && visited.insert(identity.clone())
                {
                    resolved.push(identity.clone());
                }
            }
            resolved
        };

        let packet = proto::DataPacket {
            kind: request.kind,
            destination_identities: request.destination_identities.clone(),
            value: Some(proto::data_packet::Value::User(proto::UserPacket {
                payload: request.data.clone(),
                destination_sids: request.destination_sids.clone(),
                destination_identities: request.destination_identities.clone(),
                topic: request.topic.clone(),
                nonce: request.nonce.clone(),
                ..Default::default()
            })),
            ..Default::default()
        };
        let channel_kind = if request.kind == proto::data_packet::Kind::Lossy as i32 {
            DataChannelKind::Lossy
        } else {
            DataChannelKind::Reliable
        };

        let send_result = if has_explicit_destinations {
            if resolved_destination_identities.is_empty() {
                Ok(0)
            } else {
                self.data_channels
                    .send_bytes_to_identities_with_kind(
                        &request.room,
                        &resolved_destination_identities,
                        channel_kind,
                        &packet.encode_to_vec(),
                    )
                    .await
            }
        } else {
            self.data_channels
                .send_bytes_to_room_with_kind(&request.room, channel_kind, &packet.encode_to_vec())
                .await
        };

        send_result.map(|_| ()).map_err(|err| err.to_string())
    }

    /// Disconnects an active participant due to a server-side room service action.
    pub async fn disconnect_participant_from_service(
        &self,
        room_name: &str,
        identity: &str,
        reason: proto::DisconnectReason,
    ) -> Result<(), RoomStoreError> {
        self.rooms.get_participant(room_name, identity)?;

        if let Some(outbound_tx) = self.signal_connections.get(room_name, identity) {
            let _ = outbound_tx.send(proto::SignalResponse {
                message: Some(proto::signal_response::Message::Leave(
                    proto::LeaveRequest {
                        action: proto::leave_request::Action::Disconnect as i32,
                        reason: reason as i32,
                        ..Default::default()
                    },
                )),
            });
        }

        crate::router::cleanup_participant_runtime_state(self, room_name, identity, true).await;
        Ok(())
    }

    /// Applies a service-originated participant update to signaling runtime state.
    pub fn apply_service_participant_update(
        &self,
        room_name: &str,
        previous: Option<&proto::ParticipantInfo>,
        participant: proto::ParticipantInfo,
    ) {
        if let Some(permission) = participant.permission.as_ref() {
            self.publish_permissions.set_can_publish_media(
                room_name,
                &participant.identity,
                permission.can_publish,
            );
            self.publish_permissions.set_can_publish_data(
                room_name,
                &participant.identity,
                permission.can_publish_data,
            );
            self.publish_permissions.set_can_publish_sources(
                room_name,
                &participant.identity,
                &permission
                    .can_publish_sources
                    .iter()
                    .map(|source| {
                        proto::TrackSource::try_from(*source)
                            .unwrap_or(proto::TrackSource::Unknown)
                            .as_str_name()
                            .to_ascii_lowercase()
                    })
                    .collect::<Vec<_>>(),
            );
            self.subscribe_permissions.set_can_subscribe(
                room_name,
                &participant.identity,
                permission.can_subscribe,
            );
        }

        let was_subscribe_allowed = previous
            .and_then(|info| info.permission.as_ref())
            .map(|permission| permission.can_subscribe)
            .unwrap_or(false);
        let is_subscribe_allowed = participant
            .permission
            .as_ref()
            .map(|permission| permission.can_subscribe)
            .unwrap_or(false);

        if !was_subscribe_allowed && is_subscribe_allowed {
            let state = self.clone();
            let room_name = room_name.to_string();
            let identity = participant.identity.clone();
            tokio::spawn(async move {
                crate::router::ensure_subscriber_transport_after_permission_grant(
                    &state, &room_name, &identity,
                )
                .await;
                let _ = crate::router::session::ensure_existing_media_forwarding_for_subscriber(
                    &state, &room_name, &identity,
                )
                .await;
                crate::router::session::reconcile_subscriber_data_track_subscriptions(
                    &state, &room_name, &identity,
                );
            });
        }

        if let Some(outbound_tx) = self
            .signal_connections
            .get(room_name, &participant.identity)
            && let Some(refresh_token) = self.maybe_issue_refresh_token(room_name, &participant)
        {
            tracing::debug!(
                room = room_name,
                identity = %participant.identity,
                "sending_service_refresh_token"
            );
            let _ = outbound_tx.send(proto::SignalResponse {
                message: Some(proto::signal_response::Message::RefreshToken(refresh_token)),
            });
        }

        let was_publish_allowed = previous
            .and_then(|info| info.permission.as_ref())
            .map(|permission| permission.can_publish)
            .unwrap_or(false);
        let is_publish_allowed = participant
            .permission
            .as_ref()
            .map(|permission| permission.can_publish)
            .unwrap_or(false);
        if was_publish_allowed
            && !is_publish_allowed
            && let Some(previous) = previous
        {
            let recipients = self.rooms.list_participants(room_name).unwrap_or_default();
            for track in &previous.tracks {
                if track.sid.is_empty() {
                    continue;
                }
                let response = proto::SignalResponse {
                    message: Some(proto::signal_response::Message::TrackUnpublished(
                        proto::TrackUnpublishedResponse {
                            track_sid: track.sid.clone(),
                        },
                    )),
                };
                for recipient in &recipients {
                    if let Some(outbound_tx) =
                        self.signal_connections.get(room_name, &recipient.identity)
                    {
                        let _ = outbound_tx.send(response.clone());
                    }
                }
            }
        }

        self.updates.broadcast_update(room_name, participant);
    }

    /// Broadcasts a service-originated participant update to active signalling connections.
    pub fn broadcast_participant_update_from_service(
        &self,
        room_name: &str,
        participant: proto::ParticipantInfo,
    ) {
        self.apply_service_participant_update(room_name, None, participant);
    }

    /// Creates the owner-side subscriber offer required by a relayed v0 dual-PC join.
    pub async fn create_relay_subscriber_offer(
        &self,
        room_name: &str,
        identity: &str,
        can_subscribe: bool,
        outbound_tx: &OutboundSignalSender,
    ) -> Result<proto::SignalResponse, String> {
        self.subscribe_permissions
            .set_can_subscribe(room_name, identity, can_subscribe);
        self.set_auto_subscribe_preference(room_name, identity, true);
        crate::router::session::create_subscriber_offer(
            self,
            room_name,
            identity,
            outbound_tx,
            &self.rtc_transport,
        )
        .await
        .map_err(|err| err.to_string())
    }

    /// Handles a protobuf-encoded signal request on behalf of a non-local relay worker using an external outbound sender.
    pub async fn handle_relayed_signal_request_bytes_with_outbound_sender(
        &self,
        room_name: &str,
        identity: &str,
        request: &[u8],
        outbound_tx: OutboundSignalSender,
    ) -> NonLocalRelaySignalRequestResponse {
        if let Some(raw_response) =
            crate::signal_request::raw_data_blob_response_bytes(request, self, room_name, identity)
        {
            return NonLocalRelaySignalRequestResponse::Response {
                signal_response: raw_response,
                outbound_signal_responses: Vec::new(),
            };
        }

        let request = match proto::SignalRequest::decode(request) {
            Ok(request) => request,
            Err(err) => {
                return NonLocalRelaySignalRequestResponse::Error {
                    message: format!("failed to decode relay signal request: {err}"),
                };
            }
        };

        let existing_outbound_tx = self.signal_connections.get(room_name, identity);
        let (active_outbound_tx, newly_registered_connection) =
            if let Some(existing) = existing_outbound_tx {
                (existing, false)
            } else {
                self.signal_connections
                    .insert(room_name, identity, outbound_tx.clone());
                (outbound_tx.clone(), true)
            };

        if newly_registered_connection
            && let Ok(participant) = self.rooms.get_participant(room_name, identity)
        {
            let mut updates = self
                .updates
                .register_and_broadcast_join(room_name, participant);
            let updates_tx = active_outbound_tx.clone();
            tokio::spawn(async move {
                while let Some(update) = updates.recv().await {
                    if updates_tx.send(update).is_err() {
                        break;
                    }
                }
            });

            if self
                .subscribe_permissions
                .can_subscribe(room_name, identity)
            {
                crate::router::ensure_subscriber_transport_after_permission_grant(
                    self, room_name, identity,
                )
                .await;
                let _ = crate::router::session::ensure_existing_media_forwarding_for_subscriber(
                    self, room_name, identity,
                )
                .await;
                crate::router::session::reconcile_subscriber_data_track_subscriptions(
                    self, room_name, identity,
                );
            }
        }

        match signal_response_for_request(request, self, room_name, identity, &active_outbound_tx)
            .await
        {
            Ok(Some(response)) => NonLocalRelaySignalRequestResponse::Response {
                signal_response: response.encode_to_vec(),
                outbound_signal_responses: Vec::new(),
            },
            Ok(None) => NonLocalRelaySignalRequestResponse::NoResponse,
            Err(err) if err.is_participant_left() => {
                self.signal_connections
                    .remove_if_same(room_name, identity, &outbound_tx);
                NonLocalRelaySignalRequestResponse::Closed
            }
            Err(err) => NonLocalRelaySignalRequestResponse::Error {
                message: err.to_string(),
            },
        }
    }

    /// Handles a protobuf-encoded signal request on behalf of a non-local relay worker.
    pub async fn handle_relayed_signal_request_bytes(
        &self,
        room_name: &str,
        identity: &str,
        request: &[u8],
    ) -> NonLocalRelaySignalRequestResponse {
        if let Some(raw_response) =
            crate::signal_request::raw_data_blob_response_bytes(request, self, room_name, identity)
        {
            return NonLocalRelaySignalRequestResponse::Response {
                signal_response: raw_response,
                outbound_signal_responses: Vec::new(),
            };
        }

        let request = match proto::SignalRequest::decode(request) {
            Ok(request) => request,
            Err(err) => {
                return NonLocalRelaySignalRequestResponse::Error {
                    message: format!("failed to decode relay signal request: {err}"),
                };
            }
        };

        let (outbound_tx, outbound_rx) = mpsc::unbounded_channel();
        match signal_response_for_request(request, self, room_name, identity, &outbound_tx).await {
            Ok(Some(response)) => NonLocalRelaySignalRequestResponse::Response {
                signal_response: response.encode_to_vec(),
                outbound_signal_responses: drain_relay_outbound_responses(outbound_rx).await,
            },
            Ok(None) => {
                let outbound_signal_responses = drain_relay_outbound_responses(outbound_rx).await;
                if outbound_signal_responses.is_empty() {
                    NonLocalRelaySignalRequestResponse::NoResponse
                } else {
                    NonLocalRelaySignalRequestResponse::Outbound {
                        outbound_signal_responses,
                    }
                }
            }
            Err(err) if err.is_participant_left() => NonLocalRelaySignalRequestResponse::Closed,
            Err(err) => NonLocalRelaySignalRequestResponse::Error {
                message: err.to_string(),
            },
        }
    }
}

#[cfg(test)]
mod tests {
    use std::{collections::HashSet, time::Duration};

    use jsonwebtoken::{Algorithm, EncodingKey, Header};
    use livekit_protocol as proto;
    use oxidesfu_auth::{ApiKeyStore, AuthContext, Claims, TokenVerifier, VideoGrants};
    use oxidesfu_room::RoomStore;
    use prost::Message;

    use super::SignalState;

    fn test_state() -> SignalState {
        let mut keys = ApiKeyStore::new();
        keys.insert("devkey", "secret");
        SignalState::new(RoomStore::default(), TokenVerifier::new(keys))
    }

    #[test]
    fn test_support_available_outgoing_bitrate_override_is_scoped_and_removable() {
        let state = test_state();

        assert_eq!(
            state.test_support_available_outgoing_bitrate_bps("room-a", "subscriber-a"),
            None,
            "an unset test override must preserve the production RTC-stat source"
        );

        state.set_test_support_available_outgoing_bitrate_bps(
            "room-a",
            "subscriber-a",
            Some(150_000),
        );
        state.set_test_support_available_outgoing_bitrate_bps(
            "room-a",
            "subscriber-b",
            Some(300_000),
        );

        assert_eq!(
            state.test_support_available_outgoing_bitrate_bps("room-a", "subscriber-a"),
            Some(150_000)
        );
        assert_eq!(
            state.test_support_available_outgoing_bitrate_bps("room-a", "subscriber-b"),
            Some(300_000),
            "overrides must stay isolated per subscriber"
        );
        assert_eq!(
            state.test_support_available_outgoing_bitrate_bps("room-b", "subscriber-a"),
            None,
            "overrides must stay isolated per room"
        );

        state.set_test_support_available_outgoing_bitrate_bps("room-a", "subscriber-a", None);
        assert_eq!(
            state.test_support_available_outgoing_bitrate_bps("room-a", "subscriber-a"),
            None,
            "clearing an override must restore RTC-stat allocation behavior"
        );
    }

    #[test]
    fn merge_participant_subscribe_video_mime_types_keeps_previously_known_codecs() {
        let state = test_state();
        let room = "codec-room";
        let identity = "subscriber";

        let initial = HashSet::from(["video/vp8".to_string()]);
        state.remember_participant_subscribe_video_mime_types(room, identity, &initial);
        assert!(state.participant_supports_video_mime_type(room, identity, "video/vp8"));

        let renegotiated_answer = HashSet::from(["video/h264".to_string()]);
        state.merge_participant_subscribe_video_mime_types(room, identity, &renegotiated_answer);

        assert!(
            state.participant_supports_video_mime_type(room, identity, "video/vp8"),
            "merge should preserve previously observed VP8 support"
        );
        assert!(
            state.participant_supports_video_mime_type(room, identity, "video/h264"),
            "merge should add newly observed H264 support"
        );
    }

    fn auth_context_for(room: &str, identity: &str, name: &str) -> AuthContext {
        let now = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .expect("clock should be after unix epoch")
            .as_secs() as usize;
        let claims = Claims {
            iss: "devkey".to_string(),
            exp: now + Duration::from_secs(60).as_secs() as usize,
            sub: identity.to_string(),
            name: name.to_string(),
            video: VideoGrants {
                room_join: true,
                room: room.to_string(),
                can_publish: true,
                can_subscribe: true,
                can_publish_data: true,
                can_update_own_metadata: true,
                ..Default::default()
            },
            ..Default::default()
        };

        let token = jsonwebtoken::encode(
            &Header::new(Algorithm::HS256),
            &claims,
            &EncodingKey::from_secret("secret".as_bytes()),
        )
        .expect("test token should encode");

        let mut keys = ApiKeyStore::new();
        keys.insert("devkey", "secret");
        TokenVerifier::new(keys)
            .verify_token(&token)
            .expect("token should verify")
    }

    async fn connected_data_channel_pair() -> (
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::PeerConnection,
        oxidesfu_rtc::DataChannel,
        oxidesfu_rtc::DataChannel,
    ) {
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

    #[test]
    fn reconnect_participant_retention_grace_defaults_to_ping_timeout_window() {
        let state = test_state();

        assert_eq!(
            state.reconnect_participant_retention_grace(),
            Duration::from_secs(15)
        );
    }

    #[tokio::test]
    async fn perform_rpc_from_service_round_trip_returns_destination_payload() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "rpc-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("alice should join");

        let (offer_pc, answer_pc, server_channel, client_channel) =
            connected_data_channel_pair().await;
        state.data_channels.insert_with_kind(
            "rpc-room",
            "alice",
            oxidesfu_rtc::DataChannelKind::Reliable,
            server_channel,
        );

        let state_for_task = state.clone();
        let rpc_task = tokio::spawn(async move {
            state_for_task
                .perform_rpc_from_service("rpc-room", "alice", "ping", "hello", 2_000)
                .await
        });

        let request_bytes =
            tokio::time::timeout(Duration::from_secs(3), client_channel.recv_bytes())
                .await
                .expect("rpc request should arrive before timeout")
                .expect("rpc request should be readable");
        let request_packet = proto::DataPacket::decode(request_bytes.as_slice())
            .expect("rpc request bytes should decode");
        let Some(proto::data_packet::Value::RpcRequest(request)) = request_packet.value else {
            panic!("expected rpc request packet");
        };
        assert_eq!(request.method, "ping");
        assert_eq!(request.payload, "hello");

        let consumed = state.consume_service_rpc_data_packet(&proto::DataPacket {
            participant_identity: request.id.clone(),
            value: Some(proto::data_packet::Value::RpcResponse(proto::RpcResponse {
                request_id: request.id,
                value: Some(proto::rpc_response::Value::Payload("pong".to_string())),
            })),
            ..Default::default()
        });
        assert!(
            consumed,
            "matching rpc response should be consumed by runtime"
        );

        let response = rpc_task
            .await
            .expect("rpc task should join")
            .expect("rpc should succeed");
        assert_eq!(response.payload, "pong");

        offer_pc.close().await.expect("offer pc should close");
        answer_pc.close().await.expect("answer pc should close");
    }

    #[tokio::test]
    async fn perform_rpc_from_service_times_out_without_response() {
        let state = test_state();
        state
            .rooms
            .join_participant(
                "rpc-timeout-room",
                "alice",
                "Alice",
                String::new(),
                Default::default(),
            )
            .expect("alice should join");

        let (offer_pc, answer_pc, server_channel, _client_channel) =
            connected_data_channel_pair().await;
        state.data_channels.insert_with_kind(
            "rpc-timeout-room",
            "alice",
            oxidesfu_rtc::DataChannelKind::Reliable,
            server_channel,
        );

        let result = state
            .perform_rpc_from_service("rpc-timeout-room", "alice", "slow", "hello", 25)
            .await;
        let err = result.expect_err("rpc should time out without response");
        assert!(matches!(
            err,
            oxidesfu_room::RoomStoreError::InvalidArgument(_)
        ));

        offer_pc.close().await.expect("offer pc should close");
        answer_pc.close().await.expect("answer pc should close");
    }

    #[tokio::test]
    async fn apply_service_participant_update_granting_subscribe_reconciles_data_track_handles() {
        let state = test_state();
        let room = "service-update-data-sub-room";
        let publisher = "pub";
        let subscriber = "sub";

        state
            .rooms
            .join_participant_with_permission(
                room,
                publisher,
                "Publisher",
                String::new(),
                Default::default(),
                Some(proto::ParticipantPermission {
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: true,
                    ..Default::default()
                }),
            )
            .expect("publisher should join");
        state
            .rooms
            .join_participant_with_permission(
                room,
                subscriber,
                "Subscriber",
                String::new(),
                Default::default(),
                Some(proto::ParticipantPermission {
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: false,
                    ..Default::default()
                }),
            )
            .expect("subscriber should join");

        let published = state
            .data_tracks
            .publish(
                room,
                publisher,
                &proto::PublishDataTrackRequest {
                    pub_handle: 77,
                    name: "telemetry".to_string(),
                    ..Default::default()
                },
            )
            .expect("publisher data track should publish");
        state
            .rooms
            .add_participant_data_track(room, publisher, published.clone())
            .expect("publisher data track should be added to room snapshot");

        state.set_auto_subscribe_data_track_preference(room, subscriber, true);
        let (capture_tx, mut capture_rx) =
            tokio::sync::mpsc::unbounded_channel::<proto::SignalResponse>();
        state
            .signal_connections
            .insert(room, subscriber, capture_tx);

        let previous = state
            .rooms
            .get_participant(room, subscriber)
            .expect("subscriber snapshot should exist before permission update");
        let updated = state
            .rooms
            .update_participant(
                room,
                subscriber,
                "",
                "Subscriber",
                Some(proto::ParticipantPermission {
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: true,
                    ..Default::default()
                }),
                Default::default(),
            )
            .expect("subscriber permission update should succeed");

        state.apply_service_participant_update(room, Some(&previous), updated);

        let response = tokio::time::timeout(Duration::from_secs(1), capture_rx.recv())
            .await
            .expect("data track subscriber handles should be emitted")
            .expect("data track subscriber handles response should exist");
        let Some(proto::signal_response::Message::DataTrackSubscriberHandles(handles)) =
            response.message
        else {
            panic!("expected data track subscriber handles message");
        };
        assert_eq!(handles.sub_handles.len(), 1);
        let published_track = handles
            .sub_handles
            .values()
            .next()
            .expect("published data track mapping should exist");
        assert_eq!(published_track.track_sid, published.sid);
    }

    #[tokio::test]
    async fn permission_grant_does_not_bootstrap_subscriber_transport_for_publisher_primary() {
        let state = test_state();
        let room = "publisher-primary-permission-grant-room";
        let identity = "participant";
        state.remember_participant_subscriber_primary(room, identity, false);
        let (outbound_tx, mut outbound_rx) = tokio::sync::mpsc::unbounded_channel();
        state.signal_connections.insert(room, identity, outbound_tx);

        crate::router::ensure_subscriber_transport_after_permission_grant(&state, room, identity)
            .await;

        assert!(
            tokio::time::timeout(Duration::from_millis(50), outbound_rx.recv())
                .await
                .is_err(),
            "publisher-primary permission grant must not emit a subscriber offer"
        );
        assert!(
            state
                .peer_connections
                .get(
                    room,
                    identity,
                    crate::router::SignalConnectionTarget::Subscriber
                )
                .is_none(),
            "publisher-primary permission grant must not create a subscriber peer connection"
        );
    }

    #[tokio::test]
    async fn apply_service_participant_update_granting_subscribe_bootstraps_subscriber_offer_when_missing()
     {
        let state = test_state();
        let room = "service-update-sub-offer-room";
        let subscriber = "sub";

        state
            .rooms
            .join_participant_with_permission(
                room,
                subscriber,
                "Subscriber",
                String::new(),
                Default::default(),
                Some(proto::ParticipantPermission {
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: false,
                    ..Default::default()
                }),
            )
            .expect("subscriber should join");
        state.remember_participant_auth_context(
            room,
            subscriber,
            &auth_context_for(room, subscriber, "Subscriber"),
        );
        state.remember_participant_subscriber_primary(room, subscriber, true);

        let (capture_tx, mut capture_rx) =
            tokio::sync::mpsc::unbounded_channel::<proto::SignalResponse>();
        state
            .signal_connections
            .insert(room, subscriber, capture_tx);

        let previous = state
            .rooms
            .get_participant(room, subscriber)
            .expect("subscriber snapshot should exist before permission update");
        let updated = state
            .rooms
            .update_participant(
                room,
                subscriber,
                "",
                "Subscriber",
                Some(proto::ParticipantPermission {
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: true,
                    ..Default::default()
                }),
                Default::default(),
            )
            .expect("subscriber permission update should succeed");

        state.apply_service_participant_update(room, Some(&previous), updated);

        let saw_offer = tokio::time::timeout(Duration::from_secs(3), async {
            loop {
                let Some(response) = capture_rx.recv().await else {
                    return false;
                };
                if matches!(
                    response.message,
                    Some(proto::signal_response::Message::Offer(_))
                ) {
                    break true;
                }
            }
        })
        .await
        .expect("permission update should emit signaling responses");

        assert!(saw_offer, "permission grant should emit subscriber offer");
        assert!(
            state
                .peer_connections
                .get(
                    room,
                    subscriber,
                    crate::router::SignalConnectionTarget::Subscriber
                )
                .is_some(),
            "subscriber peer connection should be created after permission grant"
        );
    }

    #[tokio::test]
    async fn apply_service_participant_update_sends_refresh_token_and_track_unpublished() {
        let state = test_state();
        let room = "service-update-room";
        let identity = "alice";

        state
            .rooms
            .join_participant_with_permission(
                room,
                identity,
                "Alice",
                String::new(),
                Default::default(),
                Some(proto::ParticipantPermission {
                    can_publish: true,
                    can_publish_data: true,
                    can_subscribe: true,
                    can_update_metadata: true,
                    ..Default::default()
                }),
            )
            .expect("participant should join");
        state.remember_participant_auth_context(
            room,
            identity,
            &auth_context_for(room, identity, "Alice"),
        );

        let previous_with_tracks = state
            .rooms
            .add_participant_track(
                room,
                identity,
                proto::TrackInfo {
                    sid: "TR_a".to_string(),
                    ..Default::default()
                },
            )
            .expect("track should add");

        let updated = state
            .rooms
            .update_participant(
                room,
                identity,
                "metadata",
                "Alice",
                Some(proto::ParticipantPermission {
                    can_publish: false,
                    can_publish_data: true,
                    can_subscribe: true,
                    can_update_metadata: true,
                    ..Default::default()
                }),
                Default::default(),
            )
            .expect("participant update should succeed");

        let (capture_tx, mut capture_rx) =
            tokio::sync::mpsc::unbounded_channel::<proto::SignalResponse>();
        state.signal_connections.insert(room, identity, capture_tx);
        state.apply_service_participant_update(room, Some(&previous_with_tracks), updated);

        let first = tokio::time::timeout(Duration::from_secs(1), capture_rx.recv())
            .await
            .expect("refresh token should be emitted")
            .expect("refresh token message should exist");
        match first.message {
            Some(proto::signal_response::Message::RefreshToken(token)) => {
                assert!(!token.is_empty(), "refresh token should not be empty");
                let mut keys = ApiKeyStore::new();
                keys.insert("devkey", "secret");
                let verified = TokenVerifier::new(keys)
                    .verify_token(&token)
                    .expect("refresh token should verify");
                assert_eq!(verified.claims.metadata, "metadata");
                assert!(!verified.claims.video.can_publish);
                assert!(verified.claims.video.can_subscribe);
                assert!(verified.claims.video.can_publish_data);
            }
            other => panic!("expected refresh token, got {other:?}"),
        }

        let second = tokio::time::timeout(Duration::from_secs(1), capture_rx.recv())
            .await
            .expect("track unpublished should be emitted")
            .expect("track unpublished message should exist");
        match second.message {
            Some(proto::signal_response::Message::TrackUnpublished(unpublished)) => {
                assert_eq!(unpublished.track_sid, "TR_a");
            }
            other => panic!("expected track unpublished, got {other:?}"),
        }
    }

    #[test]
    fn participant_data_blob_add_get_and_distinct_keys_match_contract() {
        let state = test_state().with_participant_data_blob_enabled(true);
        let room = "blob-room";
        let identity = "alice";

        let key_a = b"key-a".to_vec();
        let key_b = b"key-b".to_vec();
        let value_a = b"value-a".to_vec();
        let value_b = b"value-b".to_vec();

        state.store_participant_data_blob(room, identity, key_a.clone(), value_a.clone());
        state.store_participant_data_blob(room, identity, key_b.clone(), value_b.clone());

        assert_eq!(
            state.participant_data_blob(room, identity, &key_a),
            Some(value_a),
            "first key should round-trip independently"
        );
        assert_eq!(
            state.participant_data_blob(room, identity, &key_b),
            Some(value_b),
            "second key should round-trip independently"
        );
        assert_eq!(
            state.participant_data_blob(room, identity, b"missing-key"),
            None,
            "missing key should not resolve to a blob"
        );
    }

    #[test]
    fn participant_data_blob_add_overwrites_existing_key() {
        let state = test_state().with_participant_data_blob_enabled(true);
        let room = "blob-overwrite-room";
        let identity = "alice";
        let key = b"blob-1".to_vec();

        state.store_participant_data_blob(room, identity, key.clone(), b"v1".to_vec());
        state.store_participant_data_blob(room, identity, key.clone(), b"v2".to_vec());

        assert_eq!(
            state.participant_data_blob(room, identity, &key),
            Some(b"v2".to_vec())
        );
        assert_eq!(state.participant_data_blob_entries(room, identity).len(), 1);
    }

    #[test]
    fn participant_data_blob_delete_removes_and_missing_delete_is_noop() {
        let state = test_state().with_participant_data_blob_enabled(true);
        let room = "blob-delete-room";
        let identity = "alice";
        let key = b"blob-1".to_vec();

        state.store_participant_data_blob(room, identity, key.clone(), b"definition".to_vec());
        state.delete_participant_data_blob(room, identity, &key);

        assert_eq!(state.participant_data_blob(room, identity, &key), None);
        assert!(
            state
                .participant_data_blob_entries(room, identity)
                .is_empty()
        );

        state.delete_participant_data_blob(room, identity, &key);
        assert!(
            state
                .participant_data_blob_entries(room, identity)
                .is_empty()
        );
    }

    #[test]
    fn participant_data_blob_get_all_contents_for_participant() {
        let state = test_state().with_participant_data_blob_enabled(true);
        let room = "blob-getall-room";
        let identity = "alice";

        state.store_participant_data_blob(room, identity, b"blob-2".to_vec(), b"def-2".to_vec());
        state.store_participant_data_blob(room, identity, b"blob-1".to_vec(), b"def-1".to_vec());

        let mut all = state.participant_data_blob_entries(room, identity);
        all.sort_by(|(left, _), (right, _)| left.cmp(right));

        assert_eq!(all.len(), 2);
        assert_eq!(all[0], (b"blob-1".to_vec(), b"def-1".to_vec()));
        assert_eq!(all[1], (b"blob-2".to_vec(), b"def-2".to_vec()));
    }

    #[test]
    fn participant_data_blob_concurrent_access_matches_contract() {
        let state = std::sync::Arc::new(test_state().with_participant_data_blob_enabled(true));
        let room = "blob-concurrent-room".to_string();
        let identity = "alice".to_string();

        let mut workers = Vec::new();
        for worker_id in 0..8 {
            let state = state.clone();
            let room = room.clone();
            let identity = identity.clone();
            workers.push(std::thread::spawn(move || {
                for item_id in 0..32 {
                    let key = format!("k-{worker_id}-{item_id}").into_bytes();
                    let value = format!("v-{worker_id}-{item_id}").into_bytes();
                    state.store_participant_data_blob(&room, &identity, key.clone(), value.clone());
                    assert_eq!(
                        state.participant_data_blob(&room, &identity, &key),
                        Some(value)
                    );
                }
            }));
        }

        for worker in workers {
            worker
                .join()
                .expect("worker thread should complete without panic");
        }

        assert_eq!(
            state.participant_data_blob(&room, &identity, b"k-0-0"),
            Some(b"v-0-0".to_vec())
        );
        assert_eq!(
            state.participant_data_blob(&room, &identity, b"k-7-31"),
            Some(b"v-7-31".to_vec())
        );
    }
}
