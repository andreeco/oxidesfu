use std::sync::Arc;

use oxidesfu_room::RoomNodeDirectory;

use crate::config::set_local_room_node_draining;

/// Marks the local node as draining and signals room-cleanup shutdown.
pub fn begin_graceful_shutdown(
    directory: &Arc<dyn RoomNodeDirectory>,
    node_id: &str,
    cleanup_shutdown_tx: tokio::sync::oneshot::Sender<()>,
) {
    if let Err(err) = set_local_room_node_draining(directory, node_id, true) {
        tracing::warn!(error = %err, "failed to mark local room node as draining");
    }
    let _ = cleanup_shutdown_tx.send(());
}
