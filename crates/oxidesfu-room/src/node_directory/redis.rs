use crate::{
    RegisteredNode, RoomNodeDirectory, RoomNodeRegistryError,
    node_directory::policy::{
        DRAINING_FALSE, DRAINING_TRUE, NodeSelectorConfig, select_node_for_assignment,
    },
};

const REDIS_NODES_KEY: &str = "oxidesfu:nodes";
const REDIS_ROOM_NODES_KEY: &str = "oxidesfu:room_node_map";
const REDIS_NODE_DRAINING_KEY: &str = "oxidesfu:node_draining";

/// Minimal hash-backed storage operations required by [`RedisRoomNodeDirectory`].
pub trait RedisHashStore: Send + Sync {
    fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RoomNodeRegistryError>;
    fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RoomNodeRegistryError>;
    fn hdel(&self, key: &str, field: &str) -> Result<(), RoomNodeRegistryError>;
    fn hvals(&self, key: &str) -> Result<Vec<String>, RoomNodeRegistryError>;
}

/// Redis hash-store implementation for [`RedisRoomNodeDirectory`].
#[derive(Debug, Clone)]
pub struct RedisHashClient {
    client: redis::Client,
}

impl RedisHashClient {
    /// Builds a Redis hash client from a Redis URL.
    pub fn from_url(redis_url: &str) -> Result<Self, RoomNodeRegistryError> {
        let client =
            redis::Client::open(redis_url).map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("failed to create redis client: {err}"),
            })?;
        Ok(Self { client })
    }
}

impl RedisHashStore for RedisHashClient {
    fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RoomNodeRegistryError> {
        let mut connection =
            self.client
                .get_connection()
                .map_err(|err| RoomNodeRegistryError::Backend {
                    message: format!("failed to open redis connection: {err}"),
                })?;
        redis::cmd("HSET")
            .arg(key)
            .arg(field)
            .arg(value)
            .query::<()>(&mut connection)
            .map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("redis HSET failed: {err}"),
            })
    }

    fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RoomNodeRegistryError> {
        let mut connection =
            self.client
                .get_connection()
                .map_err(|err| RoomNodeRegistryError::Backend {
                    message: format!("failed to open redis connection: {err}"),
                })?;
        redis::cmd("HGET")
            .arg(key)
            .arg(field)
            .query::<Option<String>>(&mut connection)
            .map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("redis HGET failed: {err}"),
            })
    }

    fn hdel(&self, key: &str, field: &str) -> Result<(), RoomNodeRegistryError> {
        let mut connection =
            self.client
                .get_connection()
                .map_err(|err| RoomNodeRegistryError::Backend {
                    message: format!("failed to open redis connection: {err}"),
                })?;
        redis::cmd("HDEL")
            .arg(key)
            .arg(field)
            .query::<i64>(&mut connection)
            .map(|_| ())
            .map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("redis HDEL failed: {err}"),
            })
    }

    fn hvals(&self, key: &str) -> Result<Vec<String>, RoomNodeRegistryError> {
        let mut connection =
            self.client
                .get_connection()
                .map_err(|err| RoomNodeRegistryError::Backend {
                    message: format!("failed to open redis connection: {err}"),
                })?;
        redis::cmd("HVALS")
            .arg(key)
            .query::<Vec<String>>(&mut connection)
            .map_err(|err| RoomNodeRegistryError::Backend {
                message: format!("redis HVALS failed: {err}"),
            })
    }
}

/// Redis-backed room-node directory.
#[derive(Debug, Clone)]
pub struct RedisRoomNodeDirectory<S = RedisHashClient> {
    store: S,
    nodes_key: &'static str,
    room_nodes_key: &'static str,
    selector_config: NodeSelectorConfig,
}

impl RedisRoomNodeDirectory<RedisHashClient> {
    /// Builds a Redis-backed room-node directory from a Redis URL.
    pub fn from_redis_url(redis_url: &str) -> Result<Self, RoomNodeRegistryError> {
        let store = RedisHashClient::from_url(redis_url)?;
        Ok(Self::with_store(store))
    }

    /// Builds a Redis-backed room-node directory from a Redis URL and custom selector config.
    pub fn from_redis_url_with_selector(
        redis_url: &str,
        selector_config: NodeSelectorConfig,
    ) -> Result<Self, RoomNodeRegistryError> {
        let store = RedisHashClient::from_url(redis_url)?;
        Ok(Self::with_store_and_selector(store, selector_config))
    }
}

impl<S> RedisRoomNodeDirectory<S>
where
    S: RedisHashStore,
{
    /// Builds a room-node directory from a hash store implementation.
    pub fn with_store(store: S) -> Self {
        Self::with_store_and_selector(store, NodeSelectorConfig::default())
    }

    /// Builds a room-node directory from a hash store implementation and selector config.
    pub fn with_store_and_selector(store: S, selector_config: NodeSelectorConfig) -> Self {
        Self {
            store,
            nodes_key: REDIS_NODES_KEY,
            room_nodes_key: REDIS_ROOM_NODES_KEY,
            selector_config,
        }
    }

    fn encode_node(node: &RegisteredNode) -> Result<String, RoomNodeRegistryError> {
        serde_json::to_string(node).map_err(|err| RoomNodeRegistryError::Backend {
            message: format!("failed to encode registered node: {err}"),
        })
    }

    fn decode_node(encoded: &str) -> Result<RegisteredNode, RoomNodeRegistryError> {
        serde_json::from_str(encoded).map_err(|err| RoomNodeRegistryError::Backend {
            message: format!("failed to decode registered node: {err}"),
        })
    }
}

impl<S> RoomNodeDirectory for RedisRoomNodeDirectory<S>
where
    S: RedisHashStore,
{
    fn register_node(&self, node: RegisteredNode) -> Result<(), RoomNodeRegistryError> {
        let encoded = Self::encode_node(&node)?;
        self.store.hset(self.nodes_key, &node.id, &encoded)?;
        self.store
            .hset(REDIS_NODE_DRAINING_KEY, &node.id, DRAINING_FALSE)
    }

    fn unregister_node(&self, node_id: &str) -> Result<(), RoomNodeRegistryError> {
        self.store.hdel(self.nodes_key, node_id)?;
        self.store.hdel(REDIS_NODE_DRAINING_KEY, node_id)
    }

    fn list_nodes(&self) -> Result<Vec<RegisteredNode>, RoomNodeRegistryError> {
        let values = self.store.hvals(self.nodes_key)?;
        let mut nodes = values
            .iter()
            .map(|encoded| Self::decode_node(encoded))
            .collect::<Result<Vec<_>, _>>()?;
        nodes.sort_by(|a, b| a.id.cmp(&b.id));
        Ok(nodes)
    }

    fn set_node_for_room(
        &self,
        room_name: &str,
        node_id: &str,
    ) -> Result<(), RoomNodeRegistryError> {
        self.store.hset(self.room_nodes_key, room_name, node_id)
    }

    fn clear_room_state(&self, room_name: &str) -> Result<(), RoomNodeRegistryError> {
        self.store.hdel(self.room_nodes_key, room_name)
    }

    fn get_node_for_room(&self, room_name: &str) -> Result<RegisteredNode, RoomNodeRegistryError> {
        let Some(node_id) = self.store.hget(self.room_nodes_key, room_name)? else {
            return Err(RoomNodeRegistryError::NodeNotFound);
        };
        let Some(encoded_node) = self.store.hget(self.nodes_key, &node_id)? else {
            return Err(RoomNodeRegistryError::NodeNotFound);
        };
        Self::decode_node(&encoded_node)
    }

    fn select_or_assign_node_for_room(
        &self,
        room_name: &str,
    ) -> Result<RegisteredNode, RoomNodeRegistryError> {
        match self.get_node_for_room(room_name) {
            Ok(node) => return Ok(node),
            Err(RoomNodeRegistryError::NodeNotFound) => {}
            Err(err) => return Err(err),
        }

        let mut candidates = self.list_nodes()?;
        candidates.retain(|node| !self.is_node_draining(&node.id).unwrap_or(true));
        let selected = select_node_for_assignment(room_name, candidates, &self.selector_config)
            .ok_or(RoomNodeRegistryError::NodeNotFound)?;
        self.set_node_for_room(room_name, &selected.id)?;
        Ok(selected)
    }

    fn set_node_draining(
        &self,
        node_id: &str,
        draining: bool,
    ) -> Result<(), RoomNodeRegistryError> {
        let Some(_) = self.store.hget(self.nodes_key, node_id)? else {
            return Err(RoomNodeRegistryError::NodeNotFound);
        };
        self.store.hset(
            REDIS_NODE_DRAINING_KEY,
            node_id,
            if draining {
                DRAINING_TRUE
            } else {
                DRAINING_FALSE
            },
        )
    }

    fn is_node_draining(&self, node_id: &str) -> Result<bool, RoomNodeRegistryError> {
        let Some(_) = self.store.hget(self.nodes_key, node_id)? else {
            return Err(RoomNodeRegistryError::NodeNotFound);
        };
        let value = self
            .store
            .hget(REDIS_NODE_DRAINING_KEY, node_id)?
            .unwrap_or_else(|| DRAINING_FALSE.to_string());
        Ok(value == DRAINING_TRUE)
    }
}
