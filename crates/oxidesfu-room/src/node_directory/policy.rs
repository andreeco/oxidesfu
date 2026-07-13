use std::{
    collections::HashMap,
    hash::{Hash, Hasher},
    time::{SystemTime, UNIX_EPOCH},
};

use crate::node_directory::memory::RegisteredNode;

pub(crate) const DRAINING_FALSE: &str = "0";
pub(crate) const DRAINING_TRUE: &str = "1";

/// Node selection strategy for assigning new rooms.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeSelectorKind {
    /// Select the first node by id.
    #[default]
    First,
    /// Prefer nodes in the nearest configured region to the local node's region.
    RegionAware,
    /// Select from any available nodes using LiveKit-style sort + algorithm.
    Any,
    /// Prefer low CPU-load nodes, then sort/select with configured strategy.
    CpuLoad,
    /// Prefer low system-load nodes, then sort/select with configured strategy.
    SystemLoad,
}

/// Region coordinate used by region-aware node selection.
#[derive(Debug, Clone, PartialEq)]
pub struct SelectorRegion {
    pub name: String,
    pub lat: f64,
    pub lon: f64,
}

/// Sorting strategy used by LiveKit-style selector policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeSelectorSortBy {
    /// Pick a pseudo-random node from candidates.
    Random,
    /// Lowest per-node system load first.
    SystemLoad,
    /// Lowest CPU load first.
    #[default]
    CpuLoad,
    /// Lowest current room count first.
    Rooms,
    /// Lowest current client count first.
    Clients,
    /// Lowest current track count first.
    Tracks,
    /// Lowest current bytes-per-second first.
    BytesPerSec,
}

/// Selection algorithm used by LiveKit-style selector policies.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum NodeSelectorAlgorithm {
    /// Evaluate all candidates and return the minimum for `sort_by`.
    Lowest,
    /// Power-of-two-choices over candidates, then choose minimum for `sort_by`.
    #[default]
    TwoChoice,
}

/// Optional load/availability metrics used by load-aware node selectors.
#[derive(Debug, Clone, Copy, PartialEq, Default)]
pub struct NodeSelectionMetrics {
    /// CPU load in [0, 1+] scale.
    pub cpu_load: f32,
    /// Per-CPU system load average.
    pub system_load: f32,
    /// Number of active rooms.
    pub num_rooms: i32,
    /// Number of active clients.
    pub num_clients: i32,
    /// Number of active tracks.
    pub num_tracks: i32,
    /// Bytes per second.
    pub bytes_per_sec: i64,
    /// Whether the node is serving and eligible for assignment.
    pub serving: bool,
    /// Last metrics update time (unix seconds). `None` means available.
    pub updated_at_unix_seconds: Option<i64>,
}

/// Node selection configuration used by room-node directories.
#[derive(Debug, Clone, PartialEq)]
pub struct NodeSelectorConfig {
    pub kind: NodeSelectorKind,
    pub current_region: Option<String>,
    pub regions: Vec<SelectorRegion>,
    pub sort_by: NodeSelectorSortBy,
    pub algorithm: NodeSelectorAlgorithm,
    pub cpu_load_limit: f32,
    pub system_load_limit: f32,
    pub available_seconds: i64,
    pub now_unix_seconds: Option<i64>,
    pub metrics_by_node: HashMap<String, NodeSelectionMetrics>,
}

impl Default for NodeSelectorConfig {
    fn default() -> Self {
        Self {
            kind: NodeSelectorKind::First,
            current_region: None,
            regions: Vec::new(),
            sort_by: NodeSelectorSortBy::CpuLoad,
            algorithm: NodeSelectorAlgorithm::TwoChoice,
            cpu_load_limit: 0.8,
            system_load_limit: 1.0,
            available_seconds: 5,
            now_unix_seconds: None,
            metrics_by_node: HashMap::new(),
        }
    }
}

pub(crate) fn select_node_for_assignment(
    room_name: &str,
    candidates: impl IntoIterator<Item = RegisteredNode>,
    config: &NodeSelectorConfig,
) -> Option<RegisteredNode> {
    let mut nodes = candidates.into_iter().collect::<Vec<_>>();
    if nodes.is_empty() {
        return None;
    }

    match config.kind {
        NodeSelectorKind::First => {
            nodes.sort_by(|left, right| left.id.cmp(&right.id));
            nodes.into_iter().next()
        }
        NodeSelectorKind::RegionAware => {
            let Some(current_region) = config.current_region.as_deref() else {
                nodes.sort_by(|left, right| left.id.cmp(&right.id));
                return nodes.into_iter().next();
            };

            let mut region_lookup = HashMap::<&str, (f64, f64)>::new();
            for region in &config.regions {
                region_lookup.insert(region.name.as_str(), (region.lat, region.lon));
            }

            let Some((current_lat, current_lon)) = region_lookup.get(current_region).copied()
            else {
                nodes.sort_by(|left, right| left.id.cmp(&right.id));
                return nodes.into_iter().next();
            };

            let mut nearest_nodes = Vec::new();
            let mut nearest_distance = f64::MAX;
            for node in &nodes {
                let Some((lat, lon)) = region_lookup.get(node.region.as_str()).copied() else {
                    continue;
                };
                let distance = distance_between(current_lat, current_lon, lat, lon);
                if distance < nearest_distance {
                    nearest_distance = distance;
                    nearest_nodes.clear();
                    nearest_nodes.push(node.clone());
                } else if (distance - nearest_distance).abs() < f64::EPSILON {
                    nearest_nodes.push(node.clone());
                }
            }

            let mut pool = if nearest_nodes.is_empty() {
                nodes
            } else {
                nearest_nodes
            };
            pool.sort_by(|left, right| left.id.cmp(&right.id));
            let index = stable_room_hash_index(room_name, pool.len());
            pool.into_iter().nth(index)
        }
        NodeSelectorKind::Any => select_livekit_style(room_name, nodes, config),
        NodeSelectorKind::CpuLoad => {
            let available = available_nodes(nodes, config);
            if available.is_empty() {
                return None;
            }
            let low_load = available
                .iter()
                .filter(|node| metrics_for_node(config, &node.id).cpu_load < config.cpu_load_limit)
                .cloned()
                .collect::<Vec<_>>();
            let pool = if low_load.is_empty() {
                available
            } else {
                low_load
            };
            select_sorted_node(room_name, pool, config)
        }
        NodeSelectorKind::SystemLoad => {
            let available = available_nodes(nodes, config);
            if available.is_empty() {
                return None;
            }
            let low_load = available
                .iter()
                .filter(|node| {
                    metrics_for_node(config, &node.id).system_load < config.system_load_limit
                })
                .cloned()
                .collect::<Vec<_>>();
            let pool = if low_load.is_empty() {
                available
            } else {
                low_load
            };
            select_sorted_node(room_name, pool, config)
        }
    }
}

fn select_livekit_style(
    room_name: &str,
    nodes: Vec<RegisteredNode>,
    config: &NodeSelectorConfig,
) -> Option<RegisteredNode> {
    let available = available_nodes(nodes, config);
    if available.is_empty() {
        return None;
    }
    select_sorted_node(room_name, available, config)
}

fn available_nodes(nodes: Vec<RegisteredNode>, config: &NodeSelectorConfig) -> Vec<RegisteredNode> {
    let now_unix = current_unix_seconds(config);
    nodes
        .into_iter()
        .filter(|node| {
            let metrics = metrics_for_node(config, &node.id);
            if !metrics.serving {
                return false;
            }
            match metrics.updated_at_unix_seconds {
                Some(updated_at) => now_unix.saturating_sub(updated_at) < config.available_seconds,
                None => true,
            }
        })
        .collect()
}

fn select_sorted_node(
    room_name: &str,
    mut nodes: Vec<RegisteredNode>,
    config: &NodeSelectorConfig,
) -> Option<RegisteredNode> {
    if nodes.is_empty() {
        return None;
    }

    let select_lowest = |candidates: &mut Vec<RegisteredNode>| match config.sort_by {
        NodeSelectorSortBy::Random => {
            let idx = stable_room_hash_index(room_name, candidates.len());
            candidates.get(idx).cloned()
        }
        NodeSelectorSortBy::SystemLoad => {
            candidates.sort_by(|left, right| {
                metrics_for_node(config, &left.id)
                    .system_load
                    .total_cmp(&metrics_for_node(config, &right.id).system_load)
                    .then_with(|| left.id.cmp(&right.id))
            });
            candidates.first().cloned()
        }
        NodeSelectorSortBy::CpuLoad => {
            candidates.sort_by(|left, right| {
                metrics_for_node(config, &left.id)
                    .cpu_load
                    .total_cmp(&metrics_for_node(config, &right.id).cpu_load)
                    .then_with(|| left.id.cmp(&right.id))
            });
            candidates.first().cloned()
        }
        NodeSelectorSortBy::Rooms => {
            candidates.sort_by(|left, right| {
                metrics_for_node(config, &left.id)
                    .num_rooms
                    .cmp(&metrics_for_node(config, &right.id).num_rooms)
                    .then_with(|| left.id.cmp(&right.id))
            });
            candidates.first().cloned()
        }
        NodeSelectorSortBy::Clients => {
            candidates.sort_by(|left, right| {
                metrics_for_node(config, &left.id)
                    .num_clients
                    .cmp(&metrics_for_node(config, &right.id).num_clients)
                    .then_with(|| left.id.cmp(&right.id))
            });
            candidates.first().cloned()
        }
        NodeSelectorSortBy::Tracks => {
            candidates.sort_by(|left, right| {
                metrics_for_node(config, &left.id)
                    .num_tracks
                    .cmp(&metrics_for_node(config, &right.id).num_tracks)
                    .then_with(|| left.id.cmp(&right.id))
            });
            candidates.first().cloned()
        }
        NodeSelectorSortBy::BytesPerSec => {
            candidates.sort_by(|left, right| {
                metrics_for_node(config, &left.id)
                    .bytes_per_sec
                    .cmp(&metrics_for_node(config, &right.id).bytes_per_sec)
                    .then_with(|| left.id.cmp(&right.id))
            });
            candidates.first().cloned()
        }
    };

    match config.algorithm {
        NodeSelectorAlgorithm::Lowest => select_lowest(&mut nodes),
        NodeSelectorAlgorithm::TwoChoice => {
            if nodes.len() <= 2 {
                return select_lowest(&mut nodes);
            }
            let first_index = stable_room_hash_index(room_name, nodes.len());
            let second_seed = format!("{room_name}:second-choice");
            let mut second_index = stable_room_hash_index(&second_seed, nodes.len());
            if second_index == first_index {
                second_index = (second_index + 1) % nodes.len();
            }
            let mut two = vec![nodes[first_index].clone(), nodes[second_index].clone()];
            select_lowest(&mut two)
        }
    }
}

fn metrics_for_node(config: &NodeSelectorConfig, node_id: &str) -> NodeSelectionMetrics {
    config
        .metrics_by_node
        .get(node_id)
        .copied()
        .unwrap_or(NodeSelectionMetrics {
            serving: true,
            ..NodeSelectionMetrics::default()
        })
}

fn current_unix_seconds(config: &NodeSelectorConfig) -> i64 {
    config.now_unix_seconds.unwrap_or_else(|| {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|duration| duration.as_secs() as i64)
            .unwrap_or(0)
    })
}

fn stable_room_hash_index(room_name: &str, candidate_count: usize) -> usize {
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    room_name.hash(&mut hasher);
    (hasher.finish() as usize) % candidate_count
}

// haversine(theta) function
fn hsin(theta: f64) -> f64 {
    f64::powi(f64::sin(theta / 2.0), 2)
}

// Haversine distance in meters.
fn distance_between(lat1: f64, lon1: f64, lat2: f64, lon2: f64) -> f64 {
    const EARTH_RADIUS_METERS: f64 = 6_378_100.0;
    let lat1_rad = lat1.to_radians();
    let lon1_rad = lon1.to_radians();
    let lat2_rad = lat2.to_radians();
    let lon2_rad = lon2.to_radians();

    let h = hsin(lat2_rad - lat1_rad)
        + f64::cos(lat1_rad) * f64::cos(lat2_rad) * hsin(lon2_rad - lon1_rad);

    2.0 * EARTH_RADIUS_METERS * f64::asin(f64::sqrt(h))
}

#[cfg(test)]
mod tests {
    use super::{
        NodeSelectionMetrics, NodeSelectorAlgorithm, NodeSelectorConfig, NodeSelectorKind,
        NodeSelectorSortBy, select_node_for_assignment,
    };
    use crate::RegisteredNode;
    use std::collections::HashMap;

    fn node(id: &str) -> RegisteredNode {
        RegisteredNode {
            id: id.to_string(),
            region: "local".to_string(),
        }
    }

    #[test]
    fn any_selector_two_choice_probabilistic_behavior_favors_low_load() {
        let mut config = NodeSelectorConfig {
            kind: NodeSelectorKind::Any,
            sort_by: NodeSelectorSortBy::CpuLoad,
            algorithm: NodeSelectorAlgorithm::TwoChoice,
            now_unix_seconds: Some(10_000),
            ..NodeSelectorConfig::default()
        };
        config.metrics_by_node = HashMap::from([
            (
                "node1".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.95,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "node2".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.10,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "node3".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.50,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "node4".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.85,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
        ]);

        let mut low_load_selections = 0usize;
        let mut highest_load_selections = 0usize;

        for i in 0..1_000 {
            let room = format!("room-{i}");
            let selected = select_node_for_assignment(
                &room,
                vec![node("node1"), node("node2"), node("node3"), node("node4")],
                &config,
            )
            .expect("selector should choose an available node");

            if selected.id == "node2" {
                low_load_selections += 1;
            }
            if selected.id == "node1" {
                highest_load_selections += 1;
            }
        }

        let low_load_selection_rate = low_load_selections as f64 / 1_000.0;
        assert!(
            low_load_selection_rate > 0.4,
            "two-choice should favor the lowest-load node above random baseline"
        );
        assert_eq!(
            highest_load_selections, 0,
            "two-choice should never pick the highest-load node when lower choices exist"
        );
    }

    #[test]
    fn any_two_choice_never_picks_worst_cpu_load() {
        let mut config = NodeSelectorConfig {
            kind: NodeSelectorKind::Any,
            sort_by: NodeSelectorSortBy::CpuLoad,
            algorithm: NodeSelectorAlgorithm::TwoChoice,
            now_unix_seconds: Some(10_000),
            ..NodeSelectorConfig::default()
        };
        config.metrics_by_node = HashMap::from([
            (
                "node1".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.95,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "node2".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.10,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "node3".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.50,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "node4".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.85,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
        ]);

        for i in 0..200 {
            let room = format!("room-{i}");
            let selected = select_node_for_assignment(
                &room,
                vec![node("node1"), node("node2"), node("node3"), node("node4")],
                &config,
            )
            .expect("selector should choose an available node");
            assert_ne!(selected.id, "node1");
        }
    }

    #[test]
    fn cpu_load_selector_prefers_low_load_when_present() {
        let mut config = NodeSelectorConfig {
            kind: NodeSelectorKind::CpuLoad,
            sort_by: NodeSelectorSortBy::Random,
            algorithm: NodeSelectorAlgorithm::Lowest,
            cpu_load_limit: 0.8,
            now_unix_seconds: Some(1_000),
            ..NodeSelectorConfig::default()
        };
        config.metrics_by_node = HashMap::from([
            (
                "low".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.2,
                    serving: true,
                    updated_at_unix_seconds: Some(999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "high".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.95,
                    serving: true,
                    updated_at_unix_seconds: Some(999),
                    ..NodeSelectionMetrics::default()
                },
            ),
        ]);

        for i in 0..20 {
            let room = format!("cpu-room-{i}");
            let selected =
                select_node_for_assignment(&room, vec![node("low"), node("high")], &config)
                    .expect("cpu selector should choose node");
            assert_eq!(selected.id, "low");
        }
    }

    #[test]
    fn system_load_selector_falls_back_when_no_low_load_nodes() {
        let mut config = NodeSelectorConfig {
            kind: NodeSelectorKind::SystemLoad,
            sort_by: NodeSelectorSortBy::SystemLoad,
            algorithm: NodeSelectorAlgorithm::Lowest,
            system_load_limit: 0.1,
            now_unix_seconds: Some(2_000),
            ..NodeSelectorConfig::default()
        };
        config.metrics_by_node = HashMap::from([
            (
                "a".to_string(),
                NodeSelectionMetrics {
                    system_load: 1.2,
                    serving: true,
                    updated_at_unix_seconds: Some(1_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "b".to_string(),
                NodeSelectionMetrics {
                    system_load: 0.7,
                    serving: true,
                    updated_at_unix_seconds: Some(1_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
        ]);

        let selected = select_node_for_assignment("sys-room", vec![node("a"), node("b")], &config)
            .expect("system selector should choose lowest even when all exceed threshold");
        assert_eq!(selected.id, "b");
    }

    #[test]
    fn any_selector_excludes_stale_or_non_serving_nodes() {
        let mut config = NodeSelectorConfig {
            kind: NodeSelectorKind::Any,
            sort_by: NodeSelectorSortBy::CpuLoad,
            algorithm: NodeSelectorAlgorithm::Lowest,
            available_seconds: 5,
            now_unix_seconds: Some(10_000),
            ..NodeSelectorConfig::default()
        };
        config.metrics_by_node = HashMap::from([
            (
                "fresh".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.4,
                    serving: true,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "stale".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.1,
                    serving: true,
                    updated_at_unix_seconds: Some(9_900),
                    ..NodeSelectionMetrics::default()
                },
            ),
            (
                "down".to_string(),
                NodeSelectionMetrics {
                    cpu_load: 0.0,
                    serving: false,
                    updated_at_unix_seconds: Some(9_999),
                    ..NodeSelectionMetrics::default()
                },
            ),
        ]);

        let selected = select_node_for_assignment(
            "avail-room",
            vec![node("fresh"), node("stale"), node("down")],
            &config,
        )
        .expect("selector should keep only fresh serving node");

        assert_eq!(selected.id, "fresh");
    }
}
