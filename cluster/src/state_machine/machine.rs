use std::{io::Cursor, sync::Arc};

use openraft::{
    storage::RaftStateMachine, BasicNode, EntryPayload, LogId, OptionalSend, Snapshot, SnapshotMeta, StorageError, StorageIOError, StoredMembership
};

use crate::{state_machine::{StateMachineData, StoredSnapshot}, Request, Response, TypeConfig};

use super::StateMachineStore;

impl RaftStateMachine<TypeConfig> for Arc<StateMachineStore> {
    /// Snapshot builder type.
    type SnapshotBuilder = Self;

    /// Returns the last applied log id which is recorded in state machine, and the last applied
    /// membership config.
    ///
    /// ### Correctness requirements
    ///
    /// It is all right to return a membership with greater log id than the
    /// last-applied-log-id.
    /// Because upon startup, the last membership will be loaded by scanning logs from the
    /// `last-applied-log-id`.
    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<u64>>, StoredMembership<u64, BasicNode>), StorageError<u64>> {
        let state_machine = self.state_machine.read().await;
        Ok((
            state_machine.last_applied_log,
            state_machine.last_membership.clone(),
        ))
    }

    /// Apply the given payload of entries to the state machine.
    ///
    /// The Raft protocol guarantees that only logs which have been _committed_, that is, logs which
    /// have been replicated to a quorum of the cluster, will be applied to the state machine.
    ///
    /// This is where the business logic of interacting with your application\'s state machine
    /// should live. This is 100% application specific. Perhaps this is where an application
    /// specific transaction is being started, or perhaps committed. This may be where a key/value
    /// is being stored.
    ///
    /// For every entry to apply, an implementation should:
    /// - Store the log id as last applied log id.
    /// - Deal with the business logic log.
    /// - Store membership config if `RaftEntry::get_membership()` returns `Some`.
    ///
    /// Note that for a membership log, the implementation need to do nothing about it, except
    /// storing it.
    ///
    /// An implementation may choose to persist either the state machine or the snapshot:
    ///
    /// - An implementation with persistent state machine: persists the state on disk before
    ///   returning from `apply()`. So that a snapshot does not need to be persistent.
    ///
    /// - An implementation with persistent snapshot: `apply()` does not have to persist state on
    ///   disk. But every snapshot has to be persistent. And when starting up the application, the
    ///   state machine should be rebuilt from the last snapshot.
    async fn apply<I>(&mut self, entries: I) -> Result<Vec<Response>, StorageError<u64>>
    where
        I: IntoIterator<Item = openraft::Entry<TypeConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut output = vec![];

        let mut state_machine = self.state_machine.write().await;

        for entry in entries {
            log::info!(target: "mauve::state_machine::apply", "replicating {}", entry.log_id);
            
            state_machine.last_applied_log = Some(entry.log_id);
            match entry.payload {
                EntryPayload::Blank => output.push(Response::Empty {  }),
                EntryPayload::Normal(ref req) => match req {
                    Request::PutObject { collection, name, object } => {
                        let col = state_machine.data.get_collection(collection).unwrap();
                        col.put_object(&name, object.to_vec(), true).unwrap();
                        output.push(Response::PutObject { path: format!("{collection}/{name}") });
                    },
                    Request::DeleteObject { collection, name } => {
                        let col = state_machine.data.get_collection(&collection).unwrap();
                        col.delete_object(&name).unwrap();
                        output.push(Response::DeleteObject { path: format!("{collection}/{name}") });
                    },
                    Request::DeleteCollection { name } => {
                        state_machine.data.delete_collection(name).unwrap();
                        output.push(Response::DeleteCollection { path: name.clone() });
                    }
                },
                EntryPayload::Membership(ref mem) => {
                    state_machine.last_membership = StoredMembership::new(Some(entry.log_id), mem.clone());
                    output.push(Response::Empty {  });
                }
            }
        }
        Ok(output)
    }

    /// Get the snapshot builder for the state machine.
    ///
    /// Usually it returns a snapshot view of the state machine(i.e., subsequent changes to the
    /// state machine won\'t affect the return snapshot view), or just a copy of the entire state
    /// machine.
    ///
    /// The method is intentionally async to give the implementation a chance to use
    /// asynchronous sync primitives to serialize access to the common internal object, if
    /// needed.
    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        self.clone()
    }

    /// Create a new blank snapshot, returning a writable handle to the snapshot object.
    ///
    /// Openraft will use this handle to receive snapshot data.
    ///
    /// See the [storage chapter of the guide][sto] for details on log compaction / snapshotting.
    ///
    /// [sto]: crate::docs::getting_started#3-implement-raftlogstorage-and-raftstatemachine
    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<Cursor<Vec<u8>>>, StorageError<u64>> {
        Ok(Box::new(Cursor::new(Vec::new())))
    }

    /// Install a snapshot which has finished streaming from the leader.
    ///
    /// Before this method returns:
    /// - The state machine should be replaced with the new contents of the snapshot,
    /// - the input snapshot should be saved, i.e., [`Self::get_current_snapshot`] should return it.
    /// - and all other snapshots should be deleted at this point.
    ///
    /// ### snapshot
    ///
    /// A snapshot created from an earlier call to `begin_receiving_snapshot` which provided the
    /// snapshot.
    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<u64, BasicNode>,
        snapshot: Box<Cursor<Vec<u8>>>,
    ) -> Result<(), StorageError<u64>> {
        log::info!(target: "mauve::state_machine::install_snapshot", "installing snapshot bs={}", snapshot.get_ref().len());

        let new_snapshot = StoredSnapshot {
            meta: meta.clone(),
            data: snapshot.into_inner(),
        };

        let new_data = serde_json::from_slice(&new_snapshot.data)
            .map_err(|e| StorageIOError::read_snapshot(Some(new_snapshot.meta.signature()), &e))?;
        
        let new_state_machine = StateMachineData {
            last_applied_log: meta.last_log_id,
            last_membership: meta.last_membership.clone(),
            data: new_data,
        };

        todo!()
    }

    /// Get a readable handle to the current snapshot.
    ///
    /// ### implementation algorithm
    ///
    /// Implementing this method should be straightforward. Check the configured snapshot
    /// directory for any snapshot files. A proper implementation will only ever have one
    /// active snapshot, though another may exist while it is being created. As such, it is
    /// recommended to use a file naming pattern which will allow for easily distinguishing between
    /// the current live snapshot, and any new snapshot which is being created.
    ///
    /// A proper snapshot implementation will store last-applied-log-id and the
    /// last-applied-membership config as part of the snapshot, which should be decoded for
    /// creating this method\'s response data.
    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<TypeConfig>>, StorageError<u64>> {
        match &*self.current_snapshot.read().await {
            Some(snapshot) => {
                let data = snapshot.data.clone();
                Ok(Some(Snapshot {
                    meta: snapshot.meta.clone(),
                    snapshot: Box::new(Cursor::new(data)),
                }))
            },
            None => Ok(None),
        }
    }
}
