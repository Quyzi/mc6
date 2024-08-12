use std::{io::Cursor, sync::Arc};

use openraft::{RaftSnapshotBuilder, Snapshot, StorageError, StorageIOError};

use super::*;

impl RaftSnapshotBuilder<TypeConfig> for Arc<StateMachineStore> {
    /// Build snapshot
    ///
    /// A snapshot has to contain state of all applied log, including membership. Usually it is just
    /// a serialized state machine.
    ///
    /// Building snapshot can be done by:
    /// - Performing log compaction, e.g. merge log entries that operates on the same key, like a
    ///   LSM-tree does,
    /// - or by fetching a snapshot from the state machine.
    async fn build_snapshot(&mut self) -> Result<Snapshot<TypeConfig>, StorageError<u64>> {
        let state_machine = self.state_machine.read().await;
        let data = serde_json::to_vec(&state_machine.data.export())
            .map_err(|e| StorageIOError::read_state_machine(&e))?;

        let last_applied_log = state_machine.last_applied_log;
        let last_membership = state_machine.last_membership.clone();

        let mut current_snapshot = self.current_snapshot.write().await;
        drop(state_machine);

        let snapshot_idx = self.snapshot_idx.fetch_add(1, std::sync::atomic::Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = last_applied_log {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{}", snapshot_idx)
        };

        let meta = SnapshotMeta {
            last_log_id: last_applied_log,
            last_membership,
            snapshot_id,
        };

        let snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: data.clone(),
        };

        *current_snapshot = Some(snapshot);

        Ok(Snapshot {
            meta,
            snapshot: Box::new(Cursor::new(data))
        })
    }
}
