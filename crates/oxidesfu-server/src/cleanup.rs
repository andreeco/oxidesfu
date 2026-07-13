use std::{
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::Duration,
};

use oxidesfu_room::RoomStore;

/// Default interval for periodic empty-room cleanup sweeps.
pub const DEFAULT_ROOM_CLEANUP_INTERVAL: Duration = Duration::from_secs(30);
/// Default max age an empty room can live before cleanup.
pub const DEFAULT_EMPTY_ROOM_MAX_AGE: Duration = Duration::from_secs(60);

static ROOM_CLEANUP_REMOVED_TOTAL: AtomicU64 = AtomicU64::new(0);

/// Returns the total number of rooms removed by periodic cleanup tasks.
pub(crate) fn room_cleanup_removed_total() -> u64 {
    ROOM_CLEANUP_REMOVED_TOTAL.load(Ordering::Relaxed)
}

/// Spawns a periodic cleanup task that removes stale empty rooms.
pub fn spawn_room_cleanup_task(
    rooms: RoomStore,
    interval: Duration,
    max_empty_age: Duration,
    shutdown: tokio::sync::oneshot::Receiver<()>,
) -> tokio::task::JoinHandle<()> {
    spawn_room_cleanup_task_with_room_finished_handler(
        rooms,
        interval,
        max_empty_age,
        shutdown,
        None,
    )
}

pub fn spawn_room_cleanup_task_with_room_finished_handler(
    rooms: RoomStore,
    interval: Duration,
    max_empty_age: Duration,
    mut shutdown: tokio::sync::oneshot::Receiver<()>,
    room_finished_handler: Option<Arc<dyn Fn(livekit_protocol::Room) + Send + Sync>>,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        let mut ticker = tokio::time::interval(interval);
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                _ = &mut shutdown => {
                    break;
                }
                _ = ticker.tick() => {
                    match rooms.cleanup_expired_empty_rooms_with_default_and_collect(max_empty_age) {
                        Ok(removed_rooms) if !removed_rooms.is_empty() => {
                            ROOM_CLEANUP_REMOVED_TOTAL.fetch_add(removed_rooms.len() as u64, Ordering::Relaxed);
                            tracing::info!(removed = removed_rooms.len(), "room_cleanup_removed_stale_empty_rooms");
                            if let Some(handler) = room_finished_handler.as_ref() {
                                for room in removed_rooms {
                                    handler(room);
                                }
                            }
                        }
                        Ok(_) => {}
                        Err(err) => {
                            tracing::warn!(error = %err, "room_cleanup_failed");
                        }
                    }
                }
            }
        }
    })
}
