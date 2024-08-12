pub mod snapshot;
pub mod machine;

use std::{collections::BTreeMap, sync::atomic::AtomicU64};

use mc6_backend::backend::Backend;
use openraft::{BasicNode, LogId, SnapshotMeta, StoredMembership};
use serde::{Deserialize, Serialize};
use tokio::sync::RwLock;

use crate::TypeConfig;

pub type LogStore = crate::log_store::LogStore<TypeConfig>;

#[derive(Debug)]
pub struct StoredSnapshot {
    pub meta: SnapshotMeta<u64, BasicNode>,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct StateMachineData {
    pub last_applied_log: Option<LogId<u64>>,
    pub last_membership: StoredMembership<u64, BasicNode>,
    pub data: Backend,
}

#[derive(Debug)]
pub struct StateMachineStore {
    pub state_machine: RwLock<StateMachineData>,
    snapshot_idx: AtomicU64,
    current_snapshot: RwLock<Option<StoredSnapshot>>,
}