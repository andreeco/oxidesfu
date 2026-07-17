use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU32, Ordering},
    },
    thread,
    time::Duration,
};

use livekit_protocol as proto;

use super::{RoomStore, now_unix_ms};
use crate::{
    NodeSelectorConfig, NodeSelectorKind, RedisHashStore, RedisRoomNodeDirectory, RegisteredNode,
    RoomDefaults, RoomInternalCompat, RoomNodeDirectory, RoomNodeRegistry, RoomNodeRegistryError,
    RoomStoreError, SelectorRegion,
};

#[derive(Debug, Clone, Default)]
struct MockRedisHashStore {
    hashes: Arc<Mutex<HashMap<String, HashMap<String, String>>>>,
}

impl RedisHashStore for MockRedisHashStore {
    fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RoomNodeRegistryError> {
        let mut hashes = self
            .hashes
            .lock()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        hashes
            .entry(key.to_string())
            .or_default()
            .insert(field.to_string(), value.to_string());
        Ok(())
    }

    fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RoomNodeRegistryError> {
        let hashes = self
            .hashes
            .lock()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        Ok(hashes.get(key).and_then(|hash| hash.get(field)).cloned())
    }

    fn hdel(&self, key: &str, field: &str) -> Result<(), RoomNodeRegistryError> {
        let mut hashes = self
            .hashes
            .lock()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        if let Some(hash) = hashes.get_mut(key) {
            hash.remove(field);
        }
        Ok(())
    }

    fn hvals(&self, key: &str) -> Result<Vec<String>, RoomNodeRegistryError> {
        let hashes = self
            .hashes
            .lock()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        Ok(hashes
            .get(key)
            .map(|hash| hash.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default())
    }
}

fn exercise_directory_contract(directory: &dyn RoomNodeDirectory) {
    directory
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node-b should register");
    directory
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("node-a should register");

    let nodes = directory.list_nodes().expect("nodes should list");
    assert_eq!(
        nodes
            .iter()
            .map(|node| node.id.as_str())
            .collect::<Vec<_>>(),
        vec!["node-a", "node-b"]
    );

    let selected = directory
        .select_or_assign_node_for_room("parity-room")
        .expect("room should allocate");
    assert_eq!(selected.id, "node-a");

    directory
        .set_node_for_room("parity-room", "node-b")
        .expect("explicit remap should succeed");
    let mapped = directory
        .get_node_for_room("parity-room")
        .expect("mapped node should resolve");
    assert_eq!(mapped.id, "node-b");

    directory
        .set_node_draining("node-b", true)
        .expect("node-b should transition to draining");
    assert!(
        directory
            .is_node_draining("node-b")
            .expect("node-b draining state should be readable")
    );

    let retained = directory
        .select_or_assign_node_for_room("parity-room")
        .expect("draining-mapped active room should remain on mapped node");
    assert_eq!(retained.id, "node-b");

    let new_room_selected = directory
        .select_or_assign_node_for_room("parity-room-new")
        .expect("new room should avoid draining node");
    assert_eq!(new_room_selected.id, "node-a");

    directory
        .unregister_node("node-b")
        .expect("node-b should unregister");

    let replaced = directory
        .select_or_assign_node_for_room("parity-room")
        .expect("stale mapping should be replaced");
    assert_eq!(replaced.id, "node-a");

    directory
        .clear_room_state("parity-room")
        .expect("room state should clear");
    assert!(matches!(
        directory.get_node_for_room("parity-room"),
        Err(RoomNodeRegistryError::NodeNotFound)
    ));
}

fn exercise_node_churn_contract(directory: &dyn RoomNodeDirectory) {
    directory
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("node-a should register");
    directory
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node-b should register");

    directory
        .set_node_for_room("churn-room", "node-a")
        .expect("room should initially map to node-a");

    directory
        .unregister_node("node-a")
        .expect("node-a should unregister");
    let reassigned = directory
        .select_or_assign_node_for_room("churn-room")
        .expect("room should reassign to remaining node after unregister");
    assert_eq!(reassigned.id, "node-b");

    directory
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "eu-central-2".to_string(),
        })
        .expect("node-a should re-register");

    let stable = directory
        .select_or_assign_node_for_room("churn-room")
        .expect("existing valid mapping should remain stable after peer node re-registers");
    assert_eq!(stable.id, "node-b");
}

#[test]
fn room_node_directory_parity_between_in_memory_and_redis_adapter() {
    let in_memory = RoomNodeRegistry::default();
    exercise_directory_contract(&in_memory);
    exercise_node_churn_contract(&in_memory);

    let redis_adapter = RedisRoomNodeDirectory::with_store(MockRedisHashStore::default());
    exercise_directory_contract(&redis_adapter);
    exercise_node_churn_contract(&redis_adapter);
}

#[test]
fn room_node_registry_registers_lists_and_unregisters_nodes() {
    let registry = RoomNodeRegistry::default();

    registry
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node should register");
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("node should register");

    let nodes = registry.list_nodes().expect("nodes should list");
    assert_eq!(nodes.len(), 2);
    assert_eq!(nodes[0].id, "node-a");
    assert_eq!(nodes[1].id, "node-b");

    registry
        .unregister_node("node-a")
        .expect("node should unregister");
    let nodes = registry.list_nodes().expect("nodes should list");
    assert_eq!(nodes.len(), 1);
    assert_eq!(nodes[0].id, "node-b");
}

#[test]
fn room_node_registry_maps_room_to_registered_node() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node should register");
    registry
        .set_node_for_room("demo-room", "node-1")
        .expect("room should map to node");

    let node = registry
        .get_node_for_room("demo-room")
        .expect("room should resolve node");
    assert_eq!(node.id, "node-1");
    assert_eq!(node.region, "us-west");

    registry
        .clear_room_state("demo-room")
        .expect("room state should clear");
    assert!(matches!(
        registry.get_node_for_room("demo-room"),
        Err(RoomNodeRegistryError::NodeNotFound)
    ));
}

#[test]
fn room_node_registry_returns_node_not_found_for_stale_room_mapping() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node should register");
    registry
        .set_node_for_room("demo-room", "node-1")
        .expect("room should map to node");

    registry
        .unregister_node("node-1")
        .expect("node should unregister");

    assert!(matches!(
        registry.get_node_for_room("demo-room"),
        Err(RoomNodeRegistryError::NodeNotFound)
    ));
}

#[test]
fn room_node_registry_select_or_assign_keeps_existing_mapping() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("node should register");
    registry
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node should register");
    registry
        .set_node_for_room("demo-room", "node-b")
        .expect("room should map to node");

    let selected = registry
        .select_or_assign_node_for_room("demo-room")
        .expect("existing node should be kept");
    assert_eq!(selected.id, "node-b");
}

#[test]
fn room_node_registry_select_or_assign_chooses_first_node_when_unmapped() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-z".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node should register");
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node should register");

    let selected = registry
        .select_or_assign_node_for_room("new-room")
        .expect("node should be selected");
    assert_eq!(selected.id, "node-a");

    let mapped = registry
        .get_node_for_room("new-room")
        .expect("room should now be mapped");
    assert_eq!(mapped.id, "node-a");
}

#[test]
fn room_node_registry_select_or_assign_replaces_stale_mapping() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node should register");
    registry
        .set_node_for_room("demo-room", "node-b")
        .expect("room should map to node");
    registry
        .unregister_node("node-b")
        .expect("node should unregister");
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "eu-west".to_string(),
        })
        .expect("replacement node should register");

    let selected = registry
        .select_or_assign_node_for_room("demo-room")
        .expect("stale mapping should be replaced");
    assert_eq!(selected.id, "node-a");
}

#[test]
fn room_node_selection_allows_existing_room_on_draining_node() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node-a should register");
    registry
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node-b should register");

    registry
        .set_node_for_room("active-room", "node-b")
        .expect("room should map to node-b");
    registry
        .set_node_draining("node-b", true)
        .expect("node-b should transition to draining");

    let selected = registry
        .select_or_assign_node_for_room("active-room")
        .expect("active room should remain mapped to draining owner");
    assert_eq!(selected.id, "node-b");
}

#[test]
fn room_node_selection_ignores_draining_nodes_for_new_rooms() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node-a should register");
    registry
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node-b should register");
    registry
        .set_node_draining("node-a", true)
        .expect("node-a should transition to draining");

    let selected = registry
        .select_or_assign_node_for_room("new-room")
        .expect("new room should allocate on non-draining node");
    assert_eq!(selected.id, "node-b");
}

#[test]
fn regionaware_selector_prefers_nearest_region_for_new_room() {
    let registry = RoomNodeRegistry::with_selector(NodeSelectorConfig {
        kind: NodeSelectorKind::RegionAware,
        current_region: Some("eu-central".to_string()),
        regions: vec![
            SelectorRegion {
                name: "eu-central".to_string(),
                lat: 50.1109,
                lon: 8.6821,
            },
            SelectorRegion {
                name: "us-east".to_string(),
                lat: 40.7128,
                lon: -74.0060,
            },
        ],
        ..NodeSelectorConfig::default()
    });
    registry
        .register_node(RegisteredNode {
            id: "node-us".to_string(),
            region: "us-east".to_string(),
        })
        .expect("us node should register");
    registry
        .register_node(RegisteredNode {
            id: "node-eu".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("eu node should register");

    let selected = registry
        .select_or_assign_node_for_room("regional-room")
        .expect("regional room should allocate");
    assert_eq!(selected.id, "node-eu");
}

#[test]
fn regionaware_selector_ignores_draining_nearest_region_for_new_room() {
    let registry = RoomNodeRegistry::with_selector(NodeSelectorConfig {
        kind: NodeSelectorKind::RegionAware,
        current_region: Some("eu-central".to_string()),
        regions: vec![
            SelectorRegion {
                name: "eu-central".to_string(),
                lat: 50.1109,
                lon: 8.6821,
            },
            SelectorRegion {
                name: "us-east".to_string(),
                lat: 40.7128,
                lon: -74.0060,
            },
        ],
        ..NodeSelectorConfig::default()
    });
    registry
        .register_node(RegisteredNode {
            id: "node-eu".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("eu node should register");
    registry
        .register_node(RegisteredNode {
            id: "node-us".to_string(),
            region: "us-east".to_string(),
        })
        .expect("us node should register");
    registry
        .set_node_draining("node-eu", true)
        .expect("nearest node should drain");

    let selected = registry
        .select_or_assign_node_for_room("regional-room")
        .expect("regional room should allocate to remaining region");
    assert_eq!(selected.id, "node-us");
}

#[test]
fn regionaware_selector_keeps_existing_room_even_when_region_owner_drains() {
    let registry = RoomNodeRegistry::with_selector(NodeSelectorConfig {
        kind: NodeSelectorKind::RegionAware,
        current_region: Some("eu-central".to_string()),
        regions: vec![
            SelectorRegion {
                name: "eu-central".to_string(),
                lat: 50.1109,
                lon: 8.6821,
            },
            SelectorRegion {
                name: "us-east".to_string(),
                lat: 40.7128,
                lon: -74.0060,
            },
        ],
        ..NodeSelectorConfig::default()
    });
    registry
        .register_node(RegisteredNode {
            id: "node-eu".to_string(),
            region: "eu-central".to_string(),
        })
        .expect("eu node should register");
    registry
        .register_node(RegisteredNode {
            id: "node-us".to_string(),
            region: "us-east".to_string(),
        })
        .expect("us node should register");
    registry
        .set_node_for_room("active-regional-room", "node-eu")
        .expect("active room should map to eu node");
    registry
        .set_node_draining("node-eu", true)
        .expect("owner should drain");

    let selected = registry
        .select_or_assign_node_for_room("active-regional-room")
        .expect("active room should keep draining owner");
    assert_eq!(selected.id, "node-eu");
}

#[test]
fn room_assignment_reassigns_when_selected_node_lease_expires() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node-a should register");
    registry
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node-b should register");

    registry
        .set_node_for_room("lease-room", "node-a")
        .expect("room should map to node-a");
    registry
        .mark_node_heartbeat("node-a", 100)
        .expect("node-a heartbeat should update");
    registry
        .mark_node_heartbeat("node-b", 200)
        .expect("node-b heartbeat should update");

    let expired = registry
        .expire_nodes_with_heartbeat_older_than(150)
        .expect("stale node expiration should succeed");
    assert_eq!(expired, vec!["node-a".to_string()]);

    let selected = registry
        .select_or_assign_node_for_room("lease-room")
        .expect("room should reassign to healthy node");
    assert_eq!(selected.id, "node-b");
}

#[test]
fn node_restart_does_not_steal_active_room_without_reconciliation() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node-a should register");
    registry
        .register_node(RegisteredNode {
            id: "node-b".to_string(),
            region: "us-west".to_string(),
        })
        .expect("node-b should register");

    registry
        .set_node_for_room("reconcile-room", "node-b")
        .expect("room should map to node-b");

    registry
        .unregister_node("node-a")
        .expect("node-a should unregister");
    registry
        .register_node(RegisteredNode {
            id: "node-a".to_string(),
            region: "us-east-restarted".to_string(),
        })
        .expect("node-a should restart-register");

    let selected = registry
        .select_or_assign_node_for_room("reconcile-room")
        .expect("room should remain mapped to active node-b");
    assert_eq!(selected.id, "node-b");
}

#[test]
fn room_node_registry_select_or_assign_errors_when_no_nodes_registered() {
    let registry = RoomNodeRegistry::default();
    assert!(matches!(
        registry.select_or_assign_node_for_room("no-node-room"),
        Err(RoomNodeRegistryError::NodeNotFound)
    ));
}

#[test]
fn room_node_registry_keeps_mapping_across_node_restart_same_id() {
    let registry = RoomNodeRegistry::default();
    registry
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node should register");
    registry
        .set_node_for_room("restart-room", "node-1")
        .expect("room should map to node");

    registry
        .unregister_node("node-1")
        .expect("node should unregister");
    registry
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "us-east-2".to_string(),
        })
        .expect("node should re-register");

    let selected = registry
        .select_or_assign_node_for_room("restart-room")
        .expect("mapping should remain valid after restart with same node id");
    assert_eq!(selected.id, "node-1");
    assert_eq!(selected.region, "us-east-2");
}

#[test]
fn room_node_directory_trait_object_supports_registry_implementation() {
    let directory: Box<dyn RoomNodeDirectory> = Box::new(RoomNodeRegistry::default());
    directory
        .register_node(RegisteredNode {
            id: "node-1".to_string(),
            region: "us-east".to_string(),
        })
        .expect("node should register through trait object");

    let selected = directory
        .select_or_assign_node_for_room("trait-room")
        .expect("room should allocate through trait object");
    assert_eq!(selected.id, "node-1");

    let resolved = directory
        .get_node_for_room("trait-room")
        .expect("room should resolve through trait object");
    assert_eq!(resolved.id, "node-1");
}

#[test]
fn create_list_update_and_delete_room() {
    let store = RoomStore::default();

    let created = store
        .create_room(proto::CreateRoomRequest {
            name: "test-room".to_string(),
            metadata: "old".to_string(),
            empty_timeout: 30,
            ..Default::default()
        })
        .expect("room should create");

    assert_eq!(created.name, "test-room");
    assert_eq!(created.metadata, "old");
    assert_eq!(created.empty_timeout, 30);
    assert!(created.sid.starts_with("RM_"));

    let rooms = store.list_rooms(&[]).expect("rooms should list");
    assert_eq!(rooms.len(), 1);
    assert_eq!(rooms[0].name, "test-room");

    let updated = store
        .update_room_metadata("test-room", "new".to_string())
        .expect("metadata should update");
    assert_eq!(updated.metadata, "new");

    store.delete_room("test-room").expect("room should delete");
    assert_eq!(
        store.list_rooms(&[]).expect("rooms should list"),
        Vec::new()
    );
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestRoomInternal
#[test]
fn room_internal_roundtrip_matches_upstream_behavior() {
    let store = RoomStore::default();

    let room = proto::Room {
        sid: "123".to_string(),
        name: "test_room".to_string(),
        ..Default::default()
    };
    let internal = RoomInternalCompat {
        track_egress: Some(proto::AutoTrackEgress {
            filepath: "egress".to_string(),
            ..Default::default()
        }),
        participant_egress: None,
    };

    store
        .store_room_with_internal(&room, Some(internal.clone()))
        .expect("room with internal should store");
    let (actual_room, actual_internal) = store
        .load_room_with_internal("test_room")
        .expect("room should load with internal");

    assert_eq!(actual_room.sid, room.sid);
    assert_eq!(
        actual_internal
            .and_then(|value| value.track_egress)
            .map(|track| track.filepath),
        Some("egress".to_string())
    );

    store
        .store_room_with_internal(&room, None)
        .expect("room should update with cleared internal");
    let (_, actual_internal_none) = store
        .load_room_with_internal("test_room")
        .expect("room should load without internal");
    assert!(actual_internal_none.is_none());

    store
        .delete_room("test_room")
        .expect("room should delete after internal roundtrip");
}

#[test]
fn create_room_default_timeouts_match_livekit_docs() {
    let store = RoomStore::default();
    let room = store
        .create_room(proto::CreateRoomRequest {
            name: "defaults-room".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    assert_eq!(room.empty_timeout, 300);
    assert_eq!(room.departure_timeout, 20);
    assert!(!room.enabled_codecs.is_empty());
    assert!(room.creation_time > 0);
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestEgressStore
#[test]
fn egress_store_roundtrip_matches_upstream_behavior() {
    let store = RoomStore::default();
    let room_name = "egress-test";

    let info = proto::EgressInfo {
        egress_id: "EG_1".to_string(),
        room_id: "RM_1".to_string(),
        room_name: room_name.to_string(),
        status: proto::EgressStatus::EgressStarting as i32,
        request: Some(proto::egress_info::Request::RoomComposite(
            proto::RoomCompositeEgressRequest {
                room_name: room_name.to_string(),
                layout: "speaker-dark".to_string(),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    store.store_egress_info(&info).expect("egress should store");

    let loaded = store
        .load_egress_info(&info.egress_id)
        .expect("egress should load");
    assert_eq!(loaded.egress_id, info.egress_id);

    let mut info2 = proto::EgressInfo {
        egress_id: "EG_2".to_string(),
        room_id: "RM_2".to_string(),
        room_name: "another-egress-test".to_string(),
        status: proto::EgressStatus::EgressStarting as i32,
        request: Some(proto::egress_info::Request::RoomComposite(
            proto::RoomCompositeEgressRequest {
                room_name: "another-egress-test".to_string(),
                layout: "speaker-dark".to_string(),
                ..Default::default()
            },
        )),
        ..Default::default()
    };
    store
        .store_egress_info(&info2)
        .expect("second egress should store");

    info2.status = proto::EgressStatus::EgressComplete as i32;
    info2.ended_at = now_unix_ms().saturating_sub(24 * 60 * 60 * 1_000);
    store
        .update_egress_info(&info2)
        .expect("egress should update");

    let listed_all = store
        .list_egress_infos("", false)
        .expect("egress list should succeed");
    assert_eq!(listed_all.len(), 2);

    let listed_room = store
        .list_egress_infos(room_name, false)
        .expect("room egress list should succeed");
    assert_eq!(listed_room.len(), 1);

    let mut info_done = info.clone();
    info_done.status = proto::EgressStatus::EgressComplete as i32;
    info_done.ended_at = now_unix_ms().saturating_sub(24 * 60 * 60 * 1_000);
    store
        .update_egress_info(&info_done)
        .expect("egress should update");

    store
        .clean_ended_egress_infos()
        .expect("clean ended egress should succeed");

    let listed_after_clean = store
        .list_egress_infos(room_name, false)
        .expect("room egress list should succeed");
    assert!(listed_after_clean.is_empty());
}

#[test]
fn start_egress_creates_starting_record_with_stream_outputs() {
    let store = RoomStore::default();

    let started = store
        .start_egress_info(&proto::StartEgressRequest {
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
        })
        .expect("egress should start");

    assert!(started.egress_id.starts_with("EG_"));
    assert_eq!(started.status, proto::EgressStatus::EgressStarting as i32);
    assert_eq!(started.room_name, "room-a");
    assert_eq!(started.stream_results.len(), 1);
    assert_eq!(started.stream_results[0].url, "rtmp://example.com/live/a");
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestIngressStore
#[test]
fn ingress_store_roundtrip_matches_upstream_behavior() {
    let store = RoomStore::default();

    let mut info = proto::IngressInfo {
        ingress_id: "ingressId".to_string(),
        stream_key: "streamKey".to_string(),
        state: Some(proto::IngressState {
            started_at: 2,
            ..Default::default()
        }),
        ..Default::default()
    };

    store
        .store_ingress_info(&info)
        .expect("ingress should store");

    store
        .update_ingress_state(
            &info.ingress_id,
            info.state.as_ref().expect("state should exist"),
        )
        .expect("ingress state should update");

    let loaded = store
        .load_ingress_info("ingressId")
        .expect("ingress should load");
    assert_eq!(loaded.ingress_id, info.ingress_id);
    assert_eq!(loaded.stream_key, info.stream_key);
    assert_eq!(loaded.room_name, info.room_name);

    let room_list_initial = store
        .list_ingress_infos("room")
        .expect("ingress list should succeed");
    assert!(room_list_initial.is_empty());

    info.room_name = "room".to_string();
    store
        .update_ingress_info(&info)
        .expect("ingress should update");

    let room_list = store
        .list_ingress_infos("room")
        .expect("ingress list should succeed");
    assert_eq!(room_list.len(), 1);
    assert_eq!(room_list[0].ingress_id, info.ingress_id);

    info.room_name.clear();
    store
        .update_ingress_info(&info)
        .expect("ingress should update");

    let room_list_after_clear = store
        .list_ingress_infos("room")
        .expect("ingress list should succeed");
    assert!(room_list_after_clear.is_empty());

    let out_of_date = proto::IngressState {
        started_at: 1,
        ..Default::default()
    };
    let out_of_date_error = store
        .update_ingress_state(&info.ingress_id, &out_of_date)
        .expect_err("out-of-date ingress state should fail");
    assert_eq!(
        out_of_date_error,
        RoomStoreError::InvalidArgument("ingress state out of date".to_string())
    );

    let fresh = proto::IngressState {
        started_at: 3,
        ..Default::default()
    };
    store
        .update_ingress_state(&info.ingress_id, &fresh)
        .expect("newer ingress state should succeed");

    let all = store
        .list_ingress_infos("")
        .expect("ingress list should succeed");
    assert_eq!(all.len(), 1);
    assert_eq!(all[0].room_name, "");

    store
        .delete_ingress_info(&info)
        .expect("ingress should delete");
}

#[test]
fn create_update_delete_ingress_request_flow_behaves_like_service_contract() {
    let store = RoomStore::default();

    let created = store
        .create_ingress_info(&proto::CreateIngressRequest {
            input_type: proto::IngressInput::WhipInput as i32,
            name: "ingress-a".to_string(),
            room_name: "room-a".to_string(),
            participant_identity: "publisher-a".to_string(),
            ..Default::default()
        })
        .expect("ingress should create");

    assert!(created.ingress_id.starts_with("IN_"));
    assert!(created.stream_key.starts_with("SK_"));
    assert!(created.reusable);

    let updated = store
        .update_ingress_from_request(&proto::UpdateIngressRequest {
            ingress_id: created.ingress_id.clone(),
            name: "ingress-b".to_string(),
            room_name: "room-b".to_string(),
            participant_identity: "publisher-b".to_string(),
            ..Default::default()
        })
        .expect("ingress should update");
    assert_eq!(updated.name, "ingress-b");
    assert_eq!(updated.room_name, "room-b");

    let deleted = store
        .delete_ingress_by_id(&created.ingress_id)
        .expect("ingress should delete");
    assert_eq!(deleted.ingress_id, created.ingress_id);
    assert_eq!(
        deleted
            .state
            .as_ref()
            .expect("deleted ingress should include state")
            .status,
        proto::ingress_state::Status::EndpointInactive as i32
    );
    assert_eq!(
        store
            .load_ingress_info(&created.ingress_id)
            .expect_err("deleted ingress should not load"),
        RoomStoreError::IngressNotFound
    );
}

#[test]
fn create_ingress_url_input_validates_and_normalizes_url_scheme() {
    let store = RoomStore::default();

    let missing_url_error = store
        .create_ingress_info(&proto::CreateIngressRequest {
            input_type: proto::IngressInput::UrlInput as i32,
            ..Default::default()
        })
        .expect_err("url input ingress without url should fail");
    assert_eq!(
        missing_url_error,
        RoomStoreError::InvalidArgument("missing URL parameter".to_string())
    );

    let invalid_scheme_error = store
        .create_ingress_info(&proto::CreateIngressRequest {
            input_type: proto::IngressInput::UrlInput as i32,
            url: "ftp://example.com/stream".to_string(),
            ..Default::default()
        })
        .expect_err("unsupported url scheme should fail");
    assert_eq!(
        invalid_scheme_error,
        RoomStoreError::InvalidArgument("invalid url scheme ftp".to_string())
    );

    let created = store
        .create_ingress_info(&proto::CreateIngressRequest {
            input_type: proto::IngressInput::UrlInput as i32,
            url: "  https://example.com/live/stream.m3u8  ".to_string(),
            ..Default::default()
        })
        .expect("supported url scheme should create ingress");
    assert_eq!(created.url, "https://example.com/live/stream.m3u8");
    assert!(created.stream_key.is_empty());
    assert!(!created.reusable);
}

#[test]
fn update_ingress_rejects_non_reusable_and_resets_endpoint_error_state() {
    let store = RoomStore::default();

    store
        .store_ingress_info(&proto::IngressInfo {
            ingress_id: "IN_URL".to_string(),
            input_type: proto::IngressInput::UrlInput as i32,
            reusable: false,
            name: "url-ingress".to_string(),
            state: Some(proto::IngressState {
                status: proto::ingress_state::Status::EndpointInactive as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("url ingress should store");

    let non_reusable_error = store
        .update_ingress_from_request(&proto::UpdateIngressRequest {
            ingress_id: "IN_URL".to_string(),
            name: "updated".to_string(),
            ..Default::default()
        })
        .expect_err("non-reusable ingress should reject update");
    assert_eq!(
        non_reusable_error,
        RoomStoreError::InvalidArgument(
            "ingress is not reusable and cannot be modified".to_string()
        )
    );

    store
        .store_ingress_info(&proto::IngressInfo {
            ingress_id: "IN_RTMP".to_string(),
            input_type: proto::IngressInput::RtmpInput as i32,
            reusable: true,
            name: "rtmp-ingress".to_string(),
            state: Some(proto::IngressState {
                status: proto::ingress_state::Status::EndpointError as i32,
                error: "network timeout".to_string(),
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("rtmp ingress should store");

    let updated = store
        .update_ingress_from_request(&proto::UpdateIngressRequest {
            ingress_id: "IN_RTMP".to_string(),
            name: "rtmp-updated".to_string(),
            ..Default::default()
        })
        .expect("reusable ingress should update");
    assert_eq!(updated.name, "rtmp-updated");
    assert_eq!(
        updated
            .state
            .as_ref()
            .expect("updated ingress should include state")
            .status,
        proto::ingress_state::Status::EndpointInactive as i32
    );

    store
        .store_ingress_info(&proto::IngressInfo {
            ingress_id: "IN_DONE".to_string(),
            input_type: proto::IngressInput::RtmpInput as i32,
            reusable: true,
            name: "ingress-complete".to_string(),
            room_name: "room-before".to_string(),
            state: Some(proto::IngressState {
                status: proto::ingress_state::Status::EndpointComplete as i32,
                ..Default::default()
            }),
            ..Default::default()
        })
        .expect("completed ingress should store");

    let unchanged = store
        .update_ingress_from_request(&proto::UpdateIngressRequest {
            ingress_id: "IN_DONE".to_string(),
            name: "should-not-apply".to_string(),
            room_name: "room-after".to_string(),
            ..Default::default()
        })
        .expect("completed ingress update should return unchanged record");
    assert_eq!(unchanged.name, "ingress-complete");
    assert_eq!(unchanged.room_name, "room-before");
}

#[test]
fn stop_egress_marks_record_complete_and_rejects_already_ended() {
    let store = RoomStore::default();
    store
        .store_egress_info(&proto::EgressInfo {
            egress_id: "EG_123".to_string(),
            status: proto::EgressStatus::EgressActive as i32,
            ended_at: 0,
            ..Default::default()
        })
        .expect("egress should store");

    let stopped = store
        .stop_egress_info("EG_123")
        .expect("active egress should stop");
    assert_eq!(stopped.status, proto::EgressStatus::EgressComplete as i32);
    assert!(stopped.ended_at > 0);

    let err = store
        .stop_egress_info("EG_123")
        .expect_err("completed egress should reject repeated stop");
    assert!(matches!(err, RoomStoreError::InvalidArgument(_)));
}

#[test]
fn update_egress_layout_updates_room_composite_request() {
    let store = RoomStore::default();
    store
        .store_egress_info(&proto::EgressInfo {
            egress_id: "EG_LAYOUT".to_string(),
            status: proto::EgressStatus::EgressActive as i32,
            request: Some(proto::egress_info::Request::RoomComposite(
                proto::RoomCompositeEgressRequest {
                    layout: "speaker-dark".to_string(),
                    ..Default::default()
                },
            )),
            ..Default::default()
        })
        .expect("egress should store");

    let updated = store
        .update_egress_layout("EG_LAYOUT", "grid-light")
        .expect("layout should update");
    let request = updated.request.expect("request should exist");
    let proto::egress_info::Request::RoomComposite(room_req) = request else {
        panic!("expected room composite request");
    };
    assert_eq!(room_req.layout, "grid-light");
}

#[test]
fn update_egress_stream_urls_adds_and_removes_active_urls() {
    let store = RoomStore::default();
    store
        .store_egress_info(&proto::EgressInfo {
            egress_id: "EG_STREAMS".to_string(),
            status: proto::EgressStatus::EgressActive as i32,
            stream_results: vec![proto::StreamInfo {
                url: "rtmp://old.example/live/stream".to_string(),
                ..Default::default()
            }],
            ..Default::default()
        })
        .expect("egress should store");

    let updated = store
        .update_egress_stream_urls(
            "EG_STREAMS",
            &["rtmp://new.example/live/stream".to_string()],
            &["rtmp://old.example/live/stream".to_string()],
        )
        .expect("stream urls should update");

    assert_eq!(updated.stream_results.len(), 1);
    assert_eq!(
        updated.stream_results[0].url,
        "rtmp://new.example/live/stream"
    );
    assert_eq!(
        updated.stream_results[0].status,
        proto::stream_info::Status::Active as i32
    );
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestParticipantPersistence
#[test]
fn participant_persistence_roundtrip_matches_upstream_behavior() {
    let store = RoomStore::default();

    store
        .create_room(proto::CreateRoomRequest {
            name: "room1".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    store
        .join_participant("room1", "test", "test", String::new(), HashMap::new())
        .expect("participant should join");

    store
        .add_participant_track(
            "room1",
            "test",
            proto::TrackInfo {
                sid: "track1".to_string(),
                r#type: proto::TrackType::Audio as i32,
                name: "audio".to_string(),
                ..Default::default()
            },
        )
        .expect("track should be added");

    let loaded = store
        .get_participant("room1", "test")
        .expect("participant should load");
    assert_eq!(loaded.identity, "test");
    assert_eq!(loaded.tracks.len(), 1);
    assert_eq!(loaded.tracks[0].sid, "track1");

    let listed = store
        .list_participants("room1")
        .expect("participants should list");
    assert_eq!(listed.len(), 1);

    store
        .remove_participant("room1", "test")
        .expect("participant should delete");

    let listed_after_delete = store
        .list_participants("room1")
        .expect("participants should list");
    assert!(listed_after_delete.is_empty());

    let missing = store
        .get_participant("room1", "test")
        .expect_err("deleted participant should not load");
    assert_eq!(missing, RoomStoreError::ParticipantNotFound);
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestRoomLock
#[test]
fn room_lock_normal_locking_roundtrip() {
    let store = RoomStore::default();
    let room_name = "myroom";
    let lock_interval = Duration::from_millis(5);

    let token = store
        .lock_room(room_name, lock_interval)
        .expect("lock should succeed");
    assert!(!token.is_empty());
    store
        .unlock_room(room_name, &token)
        .expect("unlock should succeed");
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestRoomLock
#[test]
fn room_lock_waits_before_acquiring_when_held() {
    let store = RoomStore::default();
    let room_name = "myroom";
    let lock_interval = Duration::from_millis(5);

    let token = store
        .lock_room(room_name, lock_interval)
        .expect("initial lock should succeed");

    let unlocked = Arc::new(AtomicU32::new(0));
    let unlocked_for_thread = Arc::clone(&unlocked);
    let store_for_thread = store.clone();

    let waiter = thread::spawn(move || {
        let token2 = store_for_thread
            .lock_room(room_name, lock_interval)
            .expect("second lock should wait then succeed");
        assert_eq!(unlocked_for_thread.load(Ordering::SeqCst), 1);
        store_for_thread
            .unlock_room(room_name, &token2)
            .expect("second unlock should succeed");
    });

    thread::sleep(Duration::from_millis(2));
    unlocked.store(1, Ordering::SeqCst);
    store
        .unlock_room(room_name, &token)
        .expect("first unlock should succeed");

    waiter.join().expect("waiter thread should complete");
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestRoomLock
#[test]
fn room_lock_expires_and_allows_new_token() {
    let store = RoomStore::default();
    let room_name = "myroom";
    let lock_interval = Duration::from_millis(5);

    let token = store
        .lock_room(room_name, lock_interval)
        .expect("lock should succeed");

    thread::sleep(lock_interval + Duration::from_millis(1));

    let token2 = store
        .lock_room(room_name, lock_interval)
        .expect("lock should succeed after expiry");
    assert_ne!(token, token2);

    store
        .unlock_room(room_name, &token2)
        .expect("unlock should succeed");
}

#[test]
fn join_auto_creates_room_with_default_timeouts() {
    let store = RoomStore::default();
    let (room, _participant, _existing) = store
        .join_participant("auto-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("join should auto-create room");

    assert_eq!(room.empty_timeout, 300);
    assert_eq!(room.departure_timeout, 20);
}

#[test]
fn join_participant_generates_non_empty_sid_with_livekit_prefix() {
    let store = RoomStore::default();
    let (_room, participant, _existing) = store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    assert!(!participant.sid.is_empty());
    assert!(participant.sid.starts_with("PA_"));

    let loaded = store
        .get_participant("test-room", "alice")
        .expect("participant should load");
    assert_eq!(loaded.sid, participant.sid);
}

#[test]
fn participant_rejoin_version_exceeds_the_departure_version() {
    let store = RoomStore::default();
    let (_room, first_join, _existing) = store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("first join should succeed");
    let departure = store
        .remove_participant("test-room", "alice")
        .expect("participant should leave");
    let (_room, rejoined, _existing) = store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("same identity should rejoin");

    assert_eq!(first_join.version, 0);
    assert!(
        rejoined.version > departure.version,
        "rejoin version must exceed the disconnected update so clients accept it"
    );
}

#[test]
fn participant_identity_uniqueness_is_scoped_to_room() {
    let store = RoomStore::default();
    let (_room_a, a, _existing_a) = store
        .join_participant("room-a", "alice", "Alice", String::new(), HashMap::new())
        .expect("room-a join should succeed");
    let (_room_b, b, _existing_b) = store
        .join_participant("room-b", "alice", "Alice", String::new(), HashMap::new())
        .expect("room-b join should succeed");

    assert_ne!(a.sid, b.sid);
}

#[test]
fn participant_snapshot_defaults_kind_state_and_tracks() {
    let store = RoomStore::default();
    let (_room, participant, _existing) = store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    assert_eq!(
        participant.kind,
        proto::participant_info::Kind::Standard as i32
    );
    assert!(participant.kind_details.is_empty());
    assert_eq!(
        participant.state,
        proto::participant_info::State::Joined as i32
    );
    assert!(participant.tracks.is_empty());
    assert!(participant.data_tracks.is_empty());
}

#[test]
fn participant_joined_at_is_stable_across_updates() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");
    let before = store
        .get_participant("test-room", "alice")
        .expect("participant should load");

    store
        .update_participant(
            "test-room",
            "alice",
            "meta-updated",
            "Alice Updated",
            None,
            HashMap::from([("role".to_string(), "speaker".to_string())]),
        )
        .expect("participant should update");

    let after = store
        .get_participant("test-room", "alice")
        .expect("participant should load");
    assert_eq!(after.joined_at, before.joined_at);
}

#[test]
fn hidden_participant_is_stored_with_hidden_permission() {
    let store = RoomStore::default();
    store
        .join_participant_with_permission(
            "test-room",
            "hidden",
            "Hidden",
            String::new(),
            HashMap::new(),
            Some(proto::ParticipantPermission {
                hidden: true,
                ..Default::default()
            }),
        )
        .expect("hidden participant should join");

    let hidden = store
        .get_participant("test-room", "hidden")
        .expect("hidden participant should load");
    assert_eq!(
        hidden.permission.map(|permission| permission.hidden),
        Some(true)
    );
}

#[test]
fn update_participant_metadata_rejects_over_512_kib() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    let oversized = "m".repeat(512 * 1024 + 1);
    let err = store
        .update_participant("test-room", "alice", &oversized, "", None, HashMap::new())
        .expect_err("oversized metadata should be rejected");
    assert_eq!(
        err,
        RoomStoreError::InvalidArgument("metadata exceeds 512KiB limit".to_string())
    );
}

#[test]
fn update_participant_metadata_accepts_exactly_512_kib() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    let boundary = "m".repeat(512 * 1024);
    let updated = store
        .update_participant("test-room", "alice", &boundary, "", None, HashMap::new())
        .expect("512KiB metadata should be accepted");
    assert_eq!(updated.metadata.len(), 512 * 1024);
}

#[test]
fn update_participant_attributes_reject_over_64_kib_combined_size() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    let oversized = HashMap::from([("k".to_string(), "v".repeat(64 * 1024))]);
    let err = store
        .update_participant("test-room", "alice", "", "", None, oversized)
        .expect_err("oversized attributes should be rejected");
    assert_eq!(
        err,
        RoomStoreError::InvalidArgument("attributes exceed 64KiB limit".to_string())
    );
}

#[test]
fn update_participant_attributes_accept_64_kib_combined_size() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    let accepted = HashMap::from([("k".to_string(), "v".repeat(64 * 1024 - 1))]);
    let updated = store
        .update_participant("test-room", "alice", "", "", None, accepted)
        .expect("64KiB attributes should be accepted");
    assert_eq!(
        updated.attributes.get("k").map(|value| value.len()),
        Some(64 * 1024 - 1)
    );
}

#[test]
fn configured_room_defaults_apply_to_auto_created_and_service_created_rooms() {
    let store = RoomStore::with_defaults(RoomDefaults {
        max_participants: 1,
        empty_timeout: 90,
        departure_timeout: 45,
    });

    let (auto_created, _, _) = store
        .join_participant(
            "auto-defaults",
            "alice",
            "Alice",
            String::new(),
            HashMap::new(),
        )
        .expect("first participant should create the room");
    assert_eq!(auto_created.max_participants, 1);
    assert_eq!(auto_created.empty_timeout, 90);
    assert_eq!(auto_created.departure_timeout, 45);
    assert_eq!(
        store.join_participant("auto-defaults", "bob", "Bob", String::new(), HashMap::new()),
        Err(RoomStoreError::MaxParticipantsExceeded)
    );

    let service_created = store
        .create_room(proto::CreateRoomRequest {
            name: "service-defaults".to_string(),
            ..Default::default()
        })
        .expect("service room should use configured defaults");
    assert_eq!(service_created.max_participants, 1);
    assert_eq!(service_created.empty_timeout, 90);
    assert_eq!(service_created.departure_timeout, 45);
}

#[test]
fn join_participant_enforces_room_max_participants() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "limited-room".to_string(),
            max_participants: 1,
            ..Default::default()
        })
        .expect("room should create");

    store
        .join_participant(
            "limited-room",
            "alice",
            "Alice",
            String::new(),
            HashMap::new(),
        )
        .expect("first participant should join");

    assert_eq!(
        store.join_participant("limited-room", "bob", "Bob", String::new(), HashMap::new(),),
        Err(RoomStoreError::MaxParticipantsExceeded)
    );
}

#[test]
fn join_participant_replaces_existing_identity_deterministically() {
    let store = RoomStore::default();

    store
        .join_participant(
            "test-room",
            "alice",
            "Alice One",
            "meta-one".to_string(),
            HashMap::from([("tier".to_string(), "one".to_string())]),
        )
        .expect("first join should succeed");

    store
        .join_participant(
            "test-room",
            "alice",
            "Alice Two",
            "meta-two".to_string(),
            HashMap::from([("tier".to_string(), "two".to_string())]),
        )
        .expect("duplicate identity join should replace existing participant");

    let participants = store
        .list_participants("test-room")
        .expect("participants should list");
    assert_eq!(participants.len(), 1);
    assert_eq!(participants[0].identity, "alice");
    assert_eq!(participants[0].name, "Alice Two");
    assert_eq!(participants[0].metadata, "meta-two");
    assert_eq!(
        participants[0].attributes.get("tier"),
        Some(&"two".to_string())
    );
}

#[test]
fn update_participant_applies_metadata_name_permission_and_attributes() {
    let store = RoomStore::default();
    store
        .join_participant(
            "test-room",
            "alice",
            "Alice",
            "old-meta".to_string(),
            HashMap::from([("lang".to_string(), "en".to_string())]),
        )
        .expect("participant should join");

    let updated = store
        .update_participant(
            "test-room",
            "alice",
            "new-meta",
            "Alice Updated",
            Some(proto::ParticipantPermission {
                can_subscribe: false,
                can_publish: true,
                can_publish_data: true,
                can_update_metadata: true,
                hidden: false,
                can_publish_sources: vec![],
                ..Default::default()
            }),
            HashMap::from([
                ("lang".to_string(), "de".to_string()),
                ("title".to_string(), "speaker".to_string()),
            ]),
        )
        .expect("participant update should succeed");

    assert_eq!(updated.metadata, "new-meta");
    assert_eq!(updated.name, "Alice Updated");
    assert_eq!(
        updated.attributes.get("lang").map(String::as_str),
        Some("de")
    );
    assert_eq!(
        updated.attributes.get("title").map(String::as_str),
        Some("speaker")
    );
    assert_eq!(
        updated
            .permission
            .as_ref()
            .map(|permission| permission.can_subscribe),
        Some(false)
    );

    let updated = store
        .update_participant(
            "test-room",
            "alice",
            "",
            "",
            None,
            HashMap::from([("title".to_string(), String::new())]),
        )
        .expect("participant attribute removal should succeed");
    assert!(!updated.attributes.contains_key("title"));
}

#[test]
fn update_participant_attributes_preserve_unmentioned_keys() {
    let store = RoomStore::default();
    store
        .join_participant(
            "test-room",
            "alice",
            "Alice",
            String::new(),
            HashMap::from([
                ("lang".to_string(), "en".to_string()),
                ("tier".to_string(), "gold".to_string()),
            ]),
        )
        .expect("participant should join");

    let updated = store
        .update_participant(
            "test-room",
            "alice",
            "",
            "",
            None,
            HashMap::from([("lang".to_string(), "de".to_string())]),
        )
        .expect("participant should update");

    assert_eq!(updated.attributes.get("lang"), Some(&"de".to_string()));
    assert_eq!(updated.attributes.get("tier"), Some(&"gold".to_string()));
}

#[test]
fn update_participant_revoking_can_publish_unpublishes_existing_tracks() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");
    store
        .add_participant_track(
            "test-room",
            "alice",
            proto::TrackInfo {
                sid: "TR_media".to_string(),
                ..Default::default()
            },
        )
        .expect("media track should add");
    store
        .add_participant_data_track(
            "test-room",
            "alice",
            proto::DataTrackInfo {
                sid: "TR_data".to_string(),
                ..Default::default()
            },
        )
        .expect("data track should add");

    let updated = store
        .update_participant(
            "test-room",
            "alice",
            "",
            "",
            Some(proto::ParticipantPermission {
                can_subscribe: true,
                can_publish: false,
                can_publish_data: true,
                ..Default::default()
            }),
            HashMap::new(),
        )
        .expect("participant permission should update");

    assert!(updated.tracks.is_empty());
    assert!(updated.data_tracks.is_empty());
}

#[test]
fn media_track_subscription_preferences_toggle_by_track_sid_and_publisher_sid() {
    let store = RoomStore::default();
    let (_room, publisher, _existing) = store
        .join_participant(
            "test-room",
            "publisher",
            "Publisher",
            String::new(),
            HashMap::new(),
        )
        .expect("publisher should join");
    store
        .join_participant(
            "test-room",
            "subscriber",
            "Subscriber",
            String::new(),
            HashMap::new(),
        )
        .expect("subscriber should join");
    store
        .add_participant_track(
            "test-room",
            "publisher",
            proto::TrackInfo {
                sid: "TR_test".to_string(),
                r#type: proto::TrackType::Audio as i32,
                name: "mic".to_string(),
                ..Default::default()
            },
        )
        .expect("track should add");

    assert!(store.is_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber"));

    let applied = store
        .set_media_track_subscribed_by_track_sid("test-room", "subscriber", "TR_test", false)
        .expect("subscription by track sid should succeed");
    assert!(applied);
    assert!(!store.is_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber"));

    let applied = store
        .set_media_track_subscribed_by_publisher_sid(
            "test-room",
            &publisher.sid,
            "TR_test",
            "subscriber",
            true,
        )
        .expect("subscription by publisher sid should succeed");
    assert!(applied);
    assert!(store.is_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber"));
}

#[test]
fn media_subscription_revision_advances_on_subscription_and_permission_changes() {
    let store = RoomStore::default();
    store
        .join_participant(
            "test-room",
            "publisher",
            "Publisher",
            String::new(),
            HashMap::new(),
        )
        .expect("publisher should join");
    store
        .join_participant(
            "test-room",
            "subscriber",
            "Subscriber",
            String::new(),
            HashMap::new(),
        )
        .expect("subscriber should join");
    store
        .add_participant_track(
            "test-room",
            "publisher",
            proto::TrackInfo {
                sid: "TR_test".to_string(),
                r#type: proto::TrackType::Video as i32,
                name: "cam".to_string(),
                ..Default::default()
            },
        )
        .expect("track should add");

    let initial = store.media_subscription_revision();
    store
        .set_media_track_subscribed("test-room", "publisher", "TR_test", "subscriber", false)
        .expect("should set media subscription");
    let after_subscription = store.media_subscription_revision();
    assert!(after_subscription > initial);

    store
        .update_participant(
            "test-room",
            "subscriber",
            "",
            "",
            Some(proto::ParticipantPermission {
                can_subscribe: false,
                ..Default::default()
            }),
            HashMap::new(),
        )
        .expect("participant permission should update");
    assert!(store.media_subscription_revision() > after_subscription);
}

#[test]
#[allow(deprecated)] // Verify the legacy `simulcast` field remains round-trippable.
fn track_info_roundtrip_preserves_core_fields() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    let expected = proto::TrackInfo {
        sid: "TR_testsid".to_string(),
        name: "testtrack".to_string(),
        source: proto::TrackSource::ScreenShare as i32,
        r#type: proto::TrackType::Video as i32,
        simulcast: false,
        width: 100,
        height: 80,
        muted: true,
        ..Default::default()
    };

    let participant = store
        .add_participant_track("test-room", "alice", expected.clone())
        .expect("track should add");
    let track = participant
        .tracks
        .iter()
        .find(|track| track.sid == expected.sid)
        .expect("track should be present in participant snapshot");

    assert_eq!(track.sid, expected.sid);
    assert_eq!(track.name, expected.name);
    assert_eq!(track.source, expected.source);
    assert_eq!(track.r#type, expected.r#type);
    assert_eq!(track.simulcast, expected.simulcast);
    assert_eq!(track.width, expected.width);
    assert_eq!(track.height, expected.height);
    assert_eq!(track.muted, expected.muted);
}

#[test]
fn set_participant_track_muted_updates_track_snapshot() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");
    store
        .add_participant_track(
            "test-room",
            "alice",
            proto::TrackInfo {
                sid: "TR_test".to_string(),
                r#type: proto::TrackType::Audio as i32,
                name: "mic".to_string(),
                ..Default::default()
            },
        )
        .expect("track should add");

    let muted = store
        .set_participant_track_muted("test-room", "alice", "TR_test", true)
        .expect("track should mute");
    assert!(muted.muted);

    let unmuted = store
        .set_participant_track_muted("test-room", "alice", "TR_test", false)
        .expect("track should unmute");
    assert!(!unmuted.muted);
}

#[test]
fn media_track_mid_and_remove_update_participant_snapshot() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    let track = proto::TrackInfo {
        sid: "TR_test".to_string(),
        r#type: proto::TrackType::Audio as i32,
        name: "mic".to_string(),
        ..Default::default()
    };
    let participant = store
        .add_participant_track("test-room", "alice", track)
        .expect("track should add");
    assert_eq!(participant.tracks.len(), 1);
    assert_eq!(participant.tracks[0].mid, "");

    let participant = store
        .set_participant_track_mid("test-room", "alice", "TR_test", "0")
        .expect("track mid should update");
    assert_eq!(participant.tracks[0].mid, "0");

    let participant = store
        .remove_participant_track("test-room", "alice", "TR_test")
        .expect("track should remove");
    assert!(participant.tracks.is_empty());
}

#[test]
fn move_participant_transfers_between_rooms() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "source-room".to_string(),
            ..Default::default()
        })
        .expect("source room should create");
    store
        .create_room(proto::CreateRoomRequest {
            name: "destination-room".to_string(),
            ..Default::default()
        })
        .expect("destination room should create");
    store
        .join_participant(
            "source-room",
            "alice",
            "Alice",
            String::new(),
            HashMap::new(),
        )
        .expect("participant should join source room");

    let moved = store
        .move_participant("source-room", "alice", "destination-room")
        .expect("participant should move");
    assert_eq!(moved.identity, "alice");

    let source_participants = store
        .list_participants("source-room")
        .expect("source room should list participants");
    assert!(source_participants.is_empty());

    let destination_participants = store
        .list_participants("destination-room")
        .expect("destination room should list participants");
    assert_eq!(destination_participants.len(), 1);
    assert_eq!(destination_participants[0].identity, "alice");
}

#[test]
fn room_num_participants_excludes_hidden_participants() {
    let store = RoomStore::default();
    store
        .join_participant(
            "test-room",
            "visible",
            "Visible",
            String::new(),
            HashMap::new(),
        )
        .expect("visible participant should join");
    store
        .join_participant_with_permission(
            "test-room",
            "hidden",
            "Hidden",
            String::new(),
            HashMap::new(),
            Some(proto::ParticipantPermission {
                hidden: true,
                ..Default::default()
            }),
        )
        .expect("hidden participant should join");

    let room = store
        .list_rooms(&["test-room".to_string()])
        .expect("rooms should list")
        .into_iter()
        .next()
        .expect("room should exist");
    assert_eq!(room.num_participants, 1);

    store
        .update_participant(
            "test-room",
            "hidden",
            "",
            "",
            Some(proto::ParticipantPermission {
                hidden: false,
                ..Default::default()
            }),
            HashMap::new(),
        )
        .expect("hidden flag update should succeed");

    let room = store
        .list_rooms(&["test-room".to_string()])
        .expect("rooms should list")
        .into_iter()
        .next()
        .expect("room should exist");
    assert_eq!(room.num_participants, 2);
}

#[test]
fn join_and_remove_participant_updates_room_state() {
    let store = RoomStore::default();
    let (room, participant, existing) = store
        .join_participant(
            "test-room",
            "alice",
            "Alice",
            "metadata".to_string(),
            HashMap::new(),
        )
        .expect("participant should join");
    assert_eq!(room.num_participants, 1);
    assert_eq!(participant.identity, "alice");
    assert!(existing.is_empty());

    let removed = store
        .remove_participant("test-room", "alice")
        .expect("participant should remove");
    assert_eq!(removed.identity, "alice");
    assert_eq!(
        removed.state,
        proto::participant_info::State::Disconnected as i32
    );
    assert_eq!(
        store
            .list_rooms(&["test-room".to_string()])
            .expect("room should list")[0]
            .num_participants,
        0
    );
    assert_eq!(
        store.get_participant("test-room", "alice"),
        Err(RoomStoreError::ParticipantNotFound)
    );
}

#[test]
fn remove_participant_missing_identity_returns_not_found() {
    let store = RoomStore::default();
    store
        .join_participant("test-room", "alice", "Alice", String::new(), HashMap::new())
        .expect("participant should join");

    let err = store
        .remove_participant("test-room", "missing")
        .expect_err("missing identity should return not found");
    assert_eq!(err, RoomStoreError::ParticipantNotFound);
}

#[test]
fn cleanup_empty_rooms_removes_stale_empty_rooms() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "cleanup-room".to_string(),
            empty_timeout: 1,
            ..Default::default()
        })
        .expect("room should create");

    let removed = store
        .cleanup_empty_rooms_older_than_at_ms(500, now_unix_ms().saturating_add(1000))
        .expect("cleanup should succeed");
    assert_eq!(removed, 1);
    assert_eq!(
        store.list_rooms(&[]).expect("rooms should list").len(),
        0,
        "stale empty room should be removed"
    );
}

#[test]
fn cleanup_empty_rooms_keeps_non_empty_and_fresh_empty_rooms() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "fresh-empty-room".to_string(),
            ..Default::default()
        })
        .expect("fresh room should create");
    store
        .join_participant(
            "occupied-room",
            "alice",
            "Alice",
            String::new(),
            HashMap::new(),
        )
        .expect("occupied room should join participant");

    let removed = store
        .cleanup_empty_rooms_older_than(Duration::from_secs(60))
        .expect("cleanup should succeed");
    assert_eq!(removed, 0);

    let rooms = store.list_rooms(&[]).expect("rooms should list");
    assert_eq!(rooms.len(), 2);
    assert!(rooms.iter().any(|room| room.name == "fresh-empty-room"));
    assert!(rooms.iter().any(|room| room.name == "occupied-room"));
}

#[test]
fn cleanup_empty_rooms_prefers_room_empty_timeout_over_global_default() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "short-timeout-room".to_string(),
            empty_timeout: 1,
            ..Default::default()
        })
        .expect("room should create");

    let removed = store
        .cleanup_expired_empty_rooms_with_default_at_ms(60_000, now_unix_ms().saturating_add(1_500))
        .expect("cleanup should succeed");

    assert_eq!(removed, 1);
    assert!(
        !store
            .room_exists("short-timeout-room")
            .expect("room existence should query"),
        "room-level empty_timeout should drive cleanup"
    );
}

#[test]
fn cleanup_empty_rooms_does_not_remove_active_room_until_last_participant_leaves() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "active-room".to_string(),
            empty_timeout: 1,
            departure_timeout: 1,
            ..Default::default()
        })
        .expect("room should create");
    store
        .join_participant(
            "active-room",
            "alice",
            "Alice",
            String::new(),
            HashMap::new(),
        )
        .expect("participant should join");

    let removed = store
        .cleanup_expired_empty_rooms_with_default_at_ms(
            60_000,
            now_unix_ms().saturating_add(120_000),
        )
        .expect("cleanup should succeed for non-empty room");
    assert_eq!(
        removed, 0,
        "active room must not be removed by empty-room cleanup"
    );
    assert!(
        store
            .room_exists("active-room")
            .expect("room existence should query"),
        "room should stay present while a participant is connected"
    );

    store
        .remove_participant("active-room", "alice")
        .expect("participant should remove");

    let removed = store
        .cleanup_expired_empty_rooms_with_default_at_ms(60_000, now_unix_ms().saturating_add(2_000))
        .expect("cleanup should succeed after last participant leaves");
    assert_eq!(removed, 1);
    assert!(
        !store
            .room_exists("active-room")
            .expect("room existence should query"),
        "room should be removed after departure timeout once last participant leaves"
    );
    assert!(
        store.list_rooms(&[]).expect("rooms should list").is_empty(),
        "closed room should disappear from list_rooms"
    );
}

#[test]
fn cleanup_empty_rooms_uses_empty_timeout_for_room_never_joined() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "never-joined-room".to_string(),
            empty_timeout: 10,
            departure_timeout: 1,
            ..Default::default()
        })
        .expect("room should create");

    let removed = store
        .cleanup_expired_empty_rooms_with_default_at_ms(60_000, now_unix_ms().saturating_add(1_500))
        .expect("cleanup should succeed");
    assert_eq!(
        removed, 0,
        "room never joined should use empty_timeout, not departure_timeout"
    );

    let removed = store
        .cleanup_expired_empty_rooms_with_default_at_ms(
            60_000,
            now_unix_ms().saturating_add(11_000),
        )
        .expect("cleanup should succeed");
    assert_eq!(removed, 1);
}

#[test]
fn cleanup_empty_rooms_uses_departure_timeout_after_last_participant_leaves() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "departure-timeout-room".to_string(),
            empty_timeout: 120,
            departure_timeout: 2,
            ..Default::default()
        })
        .expect("room should create");

    store
        .join_participant(
            "departure-timeout-room",
            "alice",
            "Alice",
            String::new(),
            HashMap::new(),
        )
        .expect("participant should join");
    store
        .remove_participant("departure-timeout-room", "alice")
        .expect("participant should leave");

    let removed = store
        .cleanup_expired_empty_rooms_with_default_at_ms(60_000, now_unix_ms().saturating_add(2_500))
        .expect("cleanup should succeed");

    assert_eq!(removed, 1);
    assert!(
        !store
            .room_exists("departure-timeout-room")
            .expect("room existence should query"),
        "departure_timeout should be used for previously joined empty rooms"
    );
}

#[test]
fn agent_dispatch_roundtrip_create_list_delete() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "test-room".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    let created = store
        .create_agent_dispatch(proto::CreateAgentDispatchRequest {
            room: "test-room".to_string(),
            agent_name: "ag1".to_string(),
            metadata: "md".to_string(),
            deployment: "prod".to_string(),
            attributes: HashMap::from([("tier".to_string(), "gold".to_string())]),
            ..Default::default()
        })
        .expect("agent dispatch should create");
    assert!(!created.id.is_empty());

    let listed = store
        .list_agent_dispatches("test-room", "")
        .expect("dispatches should list");
    assert_eq!(listed.len(), 1);
    assert_eq!(listed[0].id, created.id);

    let deleted = store
        .delete_agent_dispatch("test-room", &created.id)
        .expect("dispatch should delete");
    assert_eq!(deleted.id, created.id);
    assert!(
        deleted
            .state
            .as_ref()
            .is_some_and(|state| state.deleted_at > 0)
    );

    let listed_after_delete = store
        .list_agent_dispatches("test-room", "")
        .expect("dispatches should list");
    assert!(listed_after_delete.is_empty());
}

// Upstream: livekit/pkg/service/redisstore_test.go::TestAgentStore
#[test]
#[allow(deprecated)] // Verify the legacy job `namespace` field remains persisted.
fn agent_store_dispatch_and_job_records_match_upstream_behavior() {
    let store = RoomStore::default();

    let dispatch = proto::AgentDispatch {
        id: "dispatch_id".to_string(),
        agent_name: "agent_name".to_string(),
        metadata: "metadata".to_string(),
        room: "room_name".to_string(),
        state: Some(proto::AgentDispatchState {
            created_at: 1,
            deleted_at: 2,
            jobs: vec![proto::Job {
                id: "job_id".to_string(),
                dispatch_id: "dispatch_id".to_string(),
                r#type: proto::JobType::JtPublisher as i32,
                room: Some(proto::Room {
                    name: "room_name".to_string(),
                    ..Default::default()
                }),
                participant: Some(proto::ParticipantInfo {
                    identity: "identity".to_string(),
                    name: "name".to_string(),
                    ..Default::default()
                }),
                namespace: "ns".to_string(),
                metadata: "metadata".to_string(),
                agent_name: "agent_name".to_string(),
                state: Some(proto::JobState {
                    status: proto::JobStatus::JsRunning as i32,
                    started_at: 3,
                    ended_at: 4,
                    error: "error".to_string(),
                    ..Default::default()
                }),
                ..Default::default()
            }],
        }),
        ..Default::default()
    };

    store
        .store_agent_dispatch_record(&dispatch)
        .expect("dispatch should store");

    let not_found_room = store
        .list_agent_dispatch_records("not_a_room")
        .expect("list should succeed");
    assert!(not_found_room.is_empty());

    let listed_without_job = store
        .list_agent_dispatch_records("room_name")
        .expect("list should succeed");
    assert_eq!(listed_without_job.len(), 1);
    assert!(
        listed_without_job[0]
            .state
            .as_ref()
            .is_none_or(|state| state.jobs.is_empty())
    );

    let job = dispatch
        .state
        .as_ref()
        .expect("dispatch state should exist")
        .jobs
        .first()
        .expect("dispatch should have one job")
        .clone();

    store
        .store_agent_job_record(&job)
        .expect("job should store");

    let listed_with_job = store
        .list_agent_dispatch_records("room_name")
        .expect("list should succeed");
    assert_eq!(listed_with_job.len(), 1);
    let jobs = &listed_with_job[0]
        .state
        .as_ref()
        .expect("state should exist")
        .jobs;
    assert_eq!(jobs.len(), 1);
    assert!(jobs[0].room.is_none());
    let participant = jobs[0]
        .participant
        .as_ref()
        .expect("participant should exist");
    assert_eq!(participant.identity, "identity");
    assert_eq!(participant.name, "");

    store
        .delete_agent_job_record(&job)
        .expect("job should delete");

    let listed_after_job_delete = store
        .list_agent_dispatch_records("room_name")
        .expect("list should succeed");
    assert_eq!(listed_after_job_delete.len(), 1);
    assert!(
        listed_after_job_delete[0]
            .state
            .as_ref()
            .is_none_or(|state| state.jobs.is_empty())
    );

    store
        .delete_agent_dispatch_record(&dispatch)
        .expect("dispatch should delete");

    let listed_after_dispatch_delete = store
        .list_agent_dispatch_records("room_name")
        .expect("list should succeed");
    assert!(listed_after_dispatch_delete.is_empty());
}

#[test]
fn agent_dispatch_limits_enforced() {
    let store = RoomStore::default();
    store
        .create_room(proto::CreateRoomRequest {
            name: "test-room".to_string(),
            ..Default::default()
        })
        .expect("room should create");

    let oversized_metadata = store
        .create_agent_dispatch(proto::CreateAgentDispatchRequest {
            room: "test-room".to_string(),
            metadata: "m".repeat(512 * 1024 + 1),
            ..Default::default()
        })
        .expect_err("oversized metadata should be rejected");
    assert_eq!(
        oversized_metadata,
        RoomStoreError::InvalidArgument("metadata exceeds 512KiB limit".to_string())
    );

    let oversized_attributes = store
        .create_agent_dispatch(proto::CreateAgentDispatchRequest {
            room: "test-room".to_string(),
            attributes: HashMap::from([("k".to_string(), "v".repeat(64 * 1024))]),
            ..Default::default()
        })
        .expect_err("oversized attributes should be rejected");
    assert_eq!(
        oversized_attributes,
        RoomStoreError::InvalidArgument("attributes exceed 64KiB limit".to_string())
    );
}
