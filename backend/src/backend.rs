use flume::{Receiver, Sender};
use serde::Serialize;
use utoipa::ToSchema;

use crate::{
    collection::Collection,
    config::AppConfig,
    errors::MauveError,
    indexer::{Indexer, IndexerSignal},
};

#[derive(Clone)]
pub struct Backend {
    db: sled::Db,
    signals: (Sender<IndexerSignal>, Receiver<IndexerSignal>),
}

impl Backend {
    /// Open the backend from a config
    pub fn open(config: AppConfig) -> Result<Self, MauveError> {
        let config: sled::Config = config.sled.into();
        let db = config.open()?;
        let signals = flume::unbounded();

        let this = Self {
            db,
            signals: signals.clone(),
        };

        let that = this.clone();
        tokio::task::spawn(async move {
            let indexer = Indexer::initialize(that)?;
            match indexer.run(signals).await {
                Ok(_) => Ok(()),
                Err(e) => {
                    log::error!("Indexer exited with error {e}");
                    Err(e)
                }
            }
        });

        Ok(this)
    }

    /// Get a Collection by name
    pub fn get_collection(&self, name: &str) -> Result<Collection, MauveError> {
        let data = self.db.open_tree(format!("mauve_data::{name}"))?;
        let meta = self.db.open_tree(format!("mauve_meta::{name}"))?;
        let index_fwd = self.db.open_tree(format!("mauve_fwd::{name}"))?;
        let index_rev = self.db.open_tree(format!("mauve_rev::{name}"))?;
        let this = Collection {
            name: name.to_string(),
            data,
            meta,
            index_fwd,
            index_rev,
        };
        self.send_signal(IndexerSignal::Watch(this.clone()))?;
        Ok(this)
    }

    /// Get a list of all the collections stored on this Backend
    pub fn list_collections(&self) -> Result<impl IntoIterator<Item = String>, MauveError> {
        let mut collections = vec![];
        for name in self.db.tree_names() {
            let s = match String::from_utf8(name.to_vec()) {
                Ok(s) => s,
                Err(e) => {
                    log::error!(err = e.to_string(); "Error stringifying collection name");
                    continue;
                }
            };
            if s.starts_with("mauve_meta::") {
                collections.push(s.strip_prefix("mauve_meta::").unwrap().to_string());
            }
        }
        Ok(collections)
    }

    /// Delete a named collection. This cannot be undone.
    pub fn delete_collection(&self, name: &str) -> Result<String, MauveError> {
        self.send_signal(IndexerSignal::Unwatch(self.get_collection(name)?))?;
        self.db.drop_tree(format!("mauve_data::{name}"))?;
        self.db.drop_tree(format!("mauve_meta::{name}"))?;
        self.db.drop_tree(format!("mauve_fwd::{name}"))?;
        self.db.drop_tree(format!("mauve_rev::{name}"))?;
        Ok(name.to_string())
    }

    /// Get backend status
    pub fn status(&self) -> Result<BackendState, MauveError> {
        Ok(self.clone().try_into()?)
    }

    /// Get a ref to the backend sled Db
    #[allow(dead_code)]
    pub(crate) fn get_db(&self) -> &sled::Db {
        &self.db
    }

    /// Send a signal to the indexer
    pub(crate) fn send_signal(&self, s: IndexerSignal) -> Result<(), MauveError> {
        self.signals.0.send(s)?;
        Ok(())
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct TreeState {
    pub checksum: u32,
    pub name: String,
    pub len: u32,
}

impl TryInto<TreeState> for sled::Tree {
    type Error = MauveError;

    fn try_into(self) -> Result<TreeState, Self::Error> {
        let checksum = self.checksum()?;
        let len = self.len() as u32;
        let name = String::from_utf8(self.name().to_vec())?;
        Ok(TreeState {
            checksum,
            name,
            len,
        })
    }
}

#[derive(Clone, Debug, Serialize, ToSchema)]
pub struct BackendState {
    pub checksum: u32,
    pub name: String,
    pub size: u64,
    pub trees: Vec<TreeState>,
    pub recovered: bool,
}

impl TryInto<BackendState> for Backend {
    type Error = MauveError;

    fn try_into(self) -> Result<BackendState, Self::Error> {
        let name = String::from_utf8(self.db.name().to_vec())?;
        let checksum = self.db.checksum()?;
        let size = self.db.size_on_disk()?;
        let recovered = self.db.was_recovered();
        let mut trees: Vec<TreeState> = vec![];
        for tree_name in self.db.tree_names() {
            trees.push(self.db.open_tree(tree_name)?.try_into()?);
        }
        Ok(BackendState {
            checksum,
            name,
            size,
            trees,
            recovered,
        })
    }
}
