use openraft::{
    storage::LogFlushed, LogId, LogState, RaftLogId, RaftTypeConfig, StorageError, Vote,
};
use serde::{Deserialize, Serialize};
use std::fmt::Debug;
use std::hash::Hash;
use std::ops::RangeBounds;

use super::LogStoreInner;

impl<
        C: RaftTypeConfig<NodeId = C> + for<'a> Deserialize<'a> + Hash + Serialize + std::fmt::Display,
    > LogStoreInner<C>
{
    pub async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug>(
        &mut self,
        range: RB,
    ) -> Result<Vec<C::Entry>, StorageError<C>>
    where
        C::Entry: Clone,
    {
        let response = self
            .log
            .range(range.clone())
            .map(|(_, ent)| ent.clone())
            .collect::<Vec<_>>();

        Ok(response)
    }

    pub async fn get_log_state(&mut self) -> Result<LogState<C>, StorageError<C>> {
        let last = self
            .log
            .iter()
            .next_back()
            .map(|(_, ent)| *ent.get_log_id());

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

    pub async fn save_committed(
        &mut self,
        committed: Option<LogId<C::NodeId>>,
    ) -> Result<(), StorageError<C>> {
        self.committed = committed;
        Ok(())
    }

    pub async fn read_committed(&self) -> Result<Option<LogId<C::NodeId>>, StorageError<C>> {
        Ok(self.committed)
    }

    pub async fn save_vote(&mut self, vote: &Vote<C::NodeId>) -> Result<(), StorageError<C>> {
        self.vote = Some(*vote);
        Ok(())
    }

    pub async fn read_vote(&mut self) -> Result<Option<Vote<C::NodeId>>, StorageError<C>> {
        Ok(self.vote)
    }

    pub async fn append<I: IntoIterator<Item = C::Entry>>(
        &mut self,
        entries: I,
        callback: LogFlushed<C>,
    ) -> Result<(), StorageError<C>> {
        for entry in entries {
            self.log.insert(entry.get_log_id().index, entry);
        }
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    pub async fn truncate(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C>> {
        let keys = self
            .log
            .range(log_id.index..)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }

    pub async fn purge(&mut self, log_id: LogId<C::NodeId>) -> Result<(), StorageError<C>> {
        let ld = &mut self.last_purged_log_id;
        assert!(*ld <= Some(log_id));
        *ld = Some(log_id);

        let keys = self
            .log
            .range(..=log_id.index)
            .map(|(k, _v)| *k)
            .collect::<Vec<_>>();
        for key in keys {
            self.log.remove(&key);
        }

        Ok(())
    }
}
