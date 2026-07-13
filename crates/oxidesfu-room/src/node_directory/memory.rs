use std::{
    collections::HashMap,
    sync::{Arc, RwLock},
};

use serde::{Deserialize, Serialize};

use crate::{
    RoomNodeRegistryError,
    node_directory::policy::{NodeSelectorConfig, select_node_for_assignment},
};

/// A node that can host RTC room sessions.
#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RegisteredNode {
    /// Stable node identifier.
    pub id: String,
    /// Region advertised by the node.
    pub region: String,
}

/// Shared room-node directory interface used by distributed routing components.
pub trait RoomNodeDirectory: Send + Sync {
    /// Registers or updates a node by id.
    fn register_node(&self, node: RegisteredNode) -> Result<(), RoomNodeRegistryError>;

    /// Unregisters a node by id.
    fn unregister_node(&self, node_id: &str) -> Result<(), RoomNodeRegistryError>;

    /// Lists all currently registered nodes sorted by id.
    fn list_nodes(&self) -> Result<Vec<RegisteredNode>, RoomNodeRegistryError>;

    /// Assigns a room to a node id.
    fn set_node_for_room(
        &self,
        room_name: &str,
        node_id: &str,
    ) -> Result<(), RoomNodeRegistryError>;

    /// Clears room-to-node mapping for a room.
    fn clear_room_state(&self, room_name: &str) -> Result<(), RoomNodeRegistryError>;

    /// Resolves the current node for a room.
    fn get_node_for_room(&self, room_name: &str) -> Result<RegisteredNode, RoomNodeRegistryError>;

    /// Resolves a room node, allocating one when needed.
    fn select_or_assign_node_for_room(
        &self,
        room_name: &str,
    ) -> Result<RegisteredNode, RoomNodeRegistryError>;

    /// Marks whether a node is currently draining and should not receive new room allocations.
    fn set_node_draining(&self, node_id: &str, draining: bool)
    -> Result<(), RoomNodeRegistryError>;

    /// Returns whether a node is currently marked as draining.
    fn is_node_draining(&self, node_id: &str) -> Result<bool, RoomNodeRegistryError>;
}

/// Thread-safe in-memory node discovery and room allocation mapping.
#[derive(Debug, Clone, Default)]
pub struct RoomNodeRegistry {
    inner: Arc<RwLock<RoomNodeRegistryInner>>,
    selector_config: NodeSelectorConfig,
}

#[derive(Debug, Default)]
struct RoomNodeRegistryInner {
    nodes: HashMap<String, RegisteredNode>,
    room_nodes: HashMap<String, String>,
    node_draining: HashMap<String, bool>,
    node_last_heartbeat_ms: HashMap<String, i64>,
}

impl RoomNodeDirectory for RoomNodeRegistry {
    fn register_node(&self, node: RegisteredNode) -> Result<(), RoomNodeRegistryError> {
        RoomNodeRegistry::register_node(self, node)
    }

    fn unregister_node(&self, node_id: &str) -> Result<(), RoomNodeRegistryError> {
        RoomNodeRegistry::unregister_node(self, node_id)
    }

    fn list_nodes(&self) -> Result<Vec<RegisteredNode>, RoomNodeRegistryError> {
        RoomNodeRegistry::list_nodes(self)
    }

    fn set_node_for_room(
        &self,
        room_name: &str,
        node_id: &str,
    ) -> Result<(), RoomNodeRegistryError> {
        RoomNodeRegistry::set_node_for_room(self, room_name, node_id)
    }

    fn clear_room_state(&self, room_name: &str) -> Result<(), RoomNodeRegistryError> {
        RoomNodeRegistry::clear_room_state(self, room_name)
    }

    fn get_node_for_room(&self, room_name: &str) -> Result<RegisteredNode, RoomNodeRegistryError> {
        RoomNodeRegistry::get_node_for_room(self, room_name)
    }

    fn select_or_assign_node_for_room(
        &self,
        room_name: &str,
    ) -> Result<RegisteredNode, RoomNodeRegistryError> {
        RoomNodeRegistry::select_or_assign_node_for_room(self, room_name)
    }

    fn set_node_draining(
        &self,
        node_id: &str,
        draining: bool,
    ) -> Result<(), RoomNodeRegistryError> {
        RoomNodeRegistry::set_node_draining(self, node_id, draining)
    }

    fn is_node_draining(&self, node_id: &str) -> Result<bool, RoomNodeRegistryError> {
        RoomNodeRegistry::is_node_draining(self, node_id)
    }
}

impl RoomNodeRegistry {
    /// Builds a room-node registry with a custom node selector policy.
    pub fn with_selector(selector_config: NodeSelectorConfig) -> Self {
        Self {
            inner: Arc::new(RwLock::new(RoomNodeRegistryInner::default())),
            selector_config,
        }
    }

    /// Registers or updates a node by id.
    pub fn register_node(&self, node: RegisteredNode) -> Result<(), RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        inner.node_draining.insert(node.id.clone(), false);
        inner
            .node_last_heartbeat_ms
            .insert(node.id.clone(), crate::store::now_unix_ms());
        inner.nodes.insert(node.id.clone(), node);
        Ok(())
    }

    /// Unregisters a node by id.
    pub fn unregister_node(&self, node_id: &str) -> Result<(), RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        inner.nodes.remove(node_id);
        inner.node_draining.remove(node_id);
        inner.node_last_heartbeat_ms.remove(node_id);
        inner
            .room_nodes
            .retain(|_, mapped_node_id| mapped_node_id != node_id);
        Ok(())
    }

    /// Lists all currently registered nodes sorted by id.
    pub fn list_nodes(&self) -> Result<Vec<RegisteredNode>, RoomNodeRegistryError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        let mut nodes = inner.nodes.values().cloned().collect::<Vec<_>>();
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(nodes)
    }

    /// Assigns a room to a node id.
    pub fn set_node_for_room(
        &self,
        room_name: &str,
        node_id: &str,
    ) -> Result<(), RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        inner
            .room_nodes
            .insert(room_name.to_string(), node_id.to_string());
        Ok(())
    }

    /// Clears room-to-node mapping for a room.
    pub fn clear_room_state(&self, room_name: &str) -> Result<(), RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        inner.room_nodes.remove(room_name);
        Ok(())
    }

    /// Resolves the current node for a room.
    ///
    /// Returns [`RoomNodeRegistryError::NodeNotFound`] when the room has no mapping,
    /// or when it maps to a node id that is no longer registered.
    pub fn get_node_for_room(
        &self,
        room_name: &str,
    ) -> Result<RegisteredNode, RoomNodeRegistryError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        let node_id = inner
            .room_nodes
            .get(room_name)
            .ok_or(RoomNodeRegistryError::NodeNotFound)?;
        inner
            .nodes
            .get(node_id)
            .cloned()
            .ok_or(RoomNodeRegistryError::NodeNotFound)
    }

    /// Resolves a room node, allocating one when needed.
    ///
    /// Behavior:
    /// - if a room is already mapped to a currently registered node, keep it (even while draining),
    /// - otherwise, select the lexicographically-first registered non-draining node,
    /// - returns [`RoomNodeRegistryError::NodeNotFound`] when no allocatable nodes are available.
    pub fn select_or_assign_node_for_room(
        &self,
        room_name: &str,
    ) -> Result<RegisteredNode, RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;

        if let Some(node_id) = inner.room_nodes.get(room_name)
            && let Some(node) = inner.nodes.get(node_id)
        {
            return Ok(node.clone());
        }

        let selected = select_node_for_assignment(
            room_name,
            inner
                .nodes
                .values()
                .filter(|node| !inner.node_draining.get(&node.id).copied().unwrap_or(false))
                .cloned(),
            &self.selector_config,
        )
        .ok_or(RoomNodeRegistryError::NodeNotFound)?;

        inner
            .room_nodes
            .insert(room_name.to_string(), selected.id.clone());
        Ok(selected)
    }

    /// Updates heartbeat timestamp for a node.
    pub fn mark_node_heartbeat(
        &self,
        node_id: &str,
        heartbeat_unix_ms: i64,
    ) -> Result<(), RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        if !inner.nodes.contains_key(node_id) {
            return Err(RoomNodeRegistryError::NodeNotFound);
        }
        inner
            .node_last_heartbeat_ms
            .insert(node_id.to_string(), heartbeat_unix_ms);
        Ok(())
    }

    /// Expires nodes with last heartbeat older than `min_heartbeat_unix_ms`.
    pub fn expire_nodes_with_heartbeat_older_than(
        &self,
        min_heartbeat_unix_ms: i64,
    ) -> Result<Vec<String>, RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;

        let stale_node_ids = inner
            .node_last_heartbeat_ms
            .iter()
            .filter_map(|(node_id, heartbeat_ms)| {
                (*heartbeat_ms < min_heartbeat_unix_ms).then_some(node_id.clone())
            })
            .collect::<Vec<_>>();

        for node_id in &stale_node_ids {
            inner.nodes.remove(node_id);
            inner.node_draining.remove(node_id);
            inner.node_last_heartbeat_ms.remove(node_id);
        }
        inner
            .room_nodes
            .retain(|_, mapped_node_id| !stale_node_ids.contains(mapped_node_id));

        Ok(stale_node_ids)
    }

    /// Marks a node as draining/non-draining.
    pub fn set_node_draining(
        &self,
        node_id: &str,
        draining: bool,
    ) -> Result<(), RoomNodeRegistryError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        if !inner.nodes.contains_key(node_id) {
            return Err(RoomNodeRegistryError::NodeNotFound);
        }
        inner.node_draining.insert(node_id.to_string(), draining);
        Ok(())
    }

    /// Returns whether a node is marked as draining.
    pub fn is_node_draining(&self, node_id: &str) -> Result<bool, RoomNodeRegistryError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomNodeRegistryError::LockPoisoned)?;
        if !inner.nodes.contains_key(node_id) {
            return Err(RoomNodeRegistryError::NodeNotFound);
        }
        Ok(inner.node_draining.get(node_id).copied().unwrap_or(false))
    }
}
