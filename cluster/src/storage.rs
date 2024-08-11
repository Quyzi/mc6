use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fmt::{Debug, Display},
    hash::Hash,
    ops::RangeBounds,
    sync::Arc,
};
use tokio::sync::Mutex;

use openraft::{
    storage::{LogFlushed, RaftLogStorage}, LogId, LogState, Node, NodeId, OptionalSend, RaftLogId, RaftLogReader, RaftTypeConfig, StorageError, Vote
};

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

impl<C: RaftTypeConfig<NodeId = C> + for<'a> Deserialize<'a> + Hash + Serialize + std::fmt::Display>
    RaftLogReader<C> for LogStore<C>
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

impl<C: RaftTypeConfig<NodeId = C> + for<'a> Deserialize<'a> + Hash + Serialize + std::fmt::Display>
    RaftLogStorage<C> for LogStore<C>
where
    C::Entry: Clone,
{
    /// Log reader type.
    ///
    /// Log reader is used by multiple replication tasks, which read logs and send them to remote
    /// nodes.
    type LogReader = Self;
    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        let mut inner = self.inner.lock().await;
        match inner.get_log_state().await {
            Ok(res) => Ok(res),
            Err(e) => {
                log::error!(target: "mauve::raft::storage::log_storage", "failed to get log state {e}");
                Err(e.into())
            }
        }
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C>> {
        let mut inner = self.inner.lock().await;
        inner.save_committed(committed).await
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<C::NodeId>>, StorageError<C>> {
        let inner = self.inner.lock().await;
        inner.read_committed().await
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C>> {
        let mut inner = self.inner.lock().await;
        inner.save_vote(vote).await
    }

    async fn append<I>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C::NodeId>>
    where
        I: IntoIterator<Item = C::Entry>,
    {
        let mut inner = self.inner.lock().await;
        inner.append(entries, callback).await
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C>> {
        let mut inner = self.inner.lock().await;
        inner.truncate(log_id).await
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C>> {
        let mut inner = self.inner.lock().await;
        inner.purge(log_id).await
    }
    
    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }
    
    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C::NodeId>> {
        let mut inner = self.inner.lock().await;
        inner.read_vote().await
    }
}


impl<C: RaftTypeConfig<NodeId = C> + for<'a> serde::Deserialize<'a> + serde::Serialize + Display + Hash>
    LogStoreInner<C>
{
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C>>
    where
        C::Entry: Clone,
    {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, val)| val.clone())
            .collect::<Vec<_>>();
        Ok(response)
    }

    async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        let last = self.log.iter().next_back().map(|(_, ent)| *ent.get_log_id());
        
        let last_purged = self.last_purged_log_id;
        let last = match last {
            None => last_purged,
            Some(last) => Some(last),
        };
        Ok(LogState {
            last_log_id: last,
            last_purged_log_id: last_purged,
        })
    }

    async fn save_committed(&mut self, committed: Option<LogId<C::NodeId>>) -> Result<(), StorageError<C>> {
        self.committed = committed;
        Ok(())
    }

    async fn read_committed(&self) -> Result<Option<LogId<C::NodeId>>, StorageError<C>> {
        Ok(self.committed)
    }

    async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C>> {
        self.vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C>> {
        Ok(self.vote)
    }

    async fn append<I: IntoIterator<Item = C::Entry>>(&mut self, entries: I, callback: LogFlushed<C>) -> Result<(), StorageError<C>>
    {
        for entry in entries {
            self.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C>> {
        let keys = self.log.range(log_id.index..).map(|(k, _v)| *k).collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C>> {
        let ld = &mut self.last_purged_log_id;
        assert!(*ld <= Some(log_id));
        *ld = Some(log_id);

        let keys = self.log.range(..=log_id.index).map(|(k, _v)| *k).collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }
}