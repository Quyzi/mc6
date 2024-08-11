use openraft::{
    storage::{LogFlushed, RaftLogStorage},
    LogId, LogState, OptionalSend, RaftLogReader, RaftTypeConfig, StorageError, Vote,
};
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, fmt::Debug, hash::Hash, ops::RangeBounds, sync::Arc};
use tokio::sync::Mutex;

pub mod ops;

#[derive(Clone, Debug, Default)]
pub struct LogStore<C: RaftTypeConfig> {
    inner: Arc<Mutex<LogStoreInner<C>>>,
}

#[derive(Clone, Debug)]
pub struct LogStoreInner<C: RaftTypeConfig> {
    last_purged_log_id: Option<LogId<C::NodeId>>,
    log: BTreeMap<u64, C::Entry>,
    committed: Option<LogId<C::NodeId>>,
    vote: Option<Vote<C::NodeId>>,
}

impl<C: RaftTypeConfig> Default for LogStoreInner<C> {
    fn default() -> Self {
        Self {
            last_purged_log_id: None,
            log: BTreeMap::new(),
            committed: None,
            vote: None,
        }
    }
}

impl<
        C: RaftTypeConfig<NodeId = C> + for<'a> Deserialize<'a> + Hash + Serialize + std::fmt::Display,
    > RaftLogReader<C> for LogStore<C>
where
    C::Entry: Clone,
{
    /// Get a series of log entries from storage.
    ///
    /// The start value is inclusive in the search and the stop value is non-inclusive: `[start,
    /// stop)`.
    ///
    /// Entry that is not found is allowed.
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.try_get_log_entries(range).await
    }
}

impl<C: RaftTypeConfig<NodeId = C> + for<'a> Deserialize<'a> + Hash + Serialize + std::fmt::Display> RaftLogStorage<C> for LogStore<C>
where
    C::Entry: Clone,
{
    /// Log reader type.
    ///
    /// Log reader is used by multiple replication tasks, which read logs and send them to remote
    /// nodes.
    type LogReader = Self;

    /// Returns the last deleted log id and the last log id.
    ///
    /// The impl should **not** consider the applied log id in state machine.
    /// The returned `last_log_id` could be the log id of the last present log entry, or the
    /// `last_purged_log_id` if there is no entry at all.
    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.get_log_state().await
    }

    /// Get the log reader.
    ///
    /// The method is intentionally async to give the implementation a chance to use asynchronous
    /// primitives to serialize access to the common internal object, if needed.
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    /// Save vote to storage.
    ///
    /// ### To ensure correctness:
    ///
    /// The vote must be persisted on disk before returning.
    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.save_vote(vote).await
    }

    /// Return the last saved vote by [`Self::save_vote`].
    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }

    /// Append log entries and call the `callback` once logs are persisted on disk.
    ///
    /// It should returns immediately after saving the input log entries in memory, and calls the
    /// `callback` when the entries are persisted on disk, i.e., avoid blocking.
    ///
    /// This method is still async because preparing the IO is usually async.
    ///
    /// ### To ensure correctness:
    ///
    /// - When this method returns, the entries must be readable, i.e., a `LogReader` can read these
    ///   entries.
    ///
    /// - When the `callback` is called, the entries must be persisted on disk.
    ///
    ///   NOTE that: the `callback` can be called either before or after this method returns.
    ///
    /// - There must not be a **hole** in logs. Because Raft only examine the last log id to ensure
    ///   correctness.
    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().await;
        inner.append(entries, callback).await
    }

    /// Truncate logs since `log_id`, inclusive
    ///
    /// ### To ensure correctness:
    ///
    /// - It must not leave a **hole** in logs.
    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.truncate(log_id).await
    }

    /// Purge logs upto `log_id`, inclusive
    ///
    /// ### To ensure correctness:
    ///
    /// - It must not leave a **hole** in logs.
    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.purge(log_id).await
    }
}
