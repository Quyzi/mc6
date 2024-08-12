use std::fmt::Debug;

use serde::{Deserialize, Serialize};
use utoipa::ToSchema;

use crate::{collection::Collection, config::AppConfig, errors::MauveError};

#[derive(Clone)]
pub struct Backend {
    db: sled::Db,
}

impl Debug for Backend {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Backend").field("checksum", &self.db.checksum().unwrap_or(0)).finish()
    }
}

impl Backend {
    /// Open the backend from a config
    pub fn open(config: AppConfig) -> Result<Self, MauveError> {
        let config: sled::Config = config.sled.into();
        let db = config.open()?;
        Ok(Self { db })
    }

    /// Get a Collection by name
    pub fn get_collection(&self, name: &str) -> Result<Collection, MauveError> {
        let name = format!("mauve::{name}");
        let tree = self.db.open_tree(name)?;
        Ok(tree.into())
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
            if s.starts_with("mauve::") {
                collections.push(s.strip_prefix("mauve::").unwrap().to_string());
            }
        }
        Ok(collections)
    }

    pub fn export(&self) -> Vec<(Vec<u8>, Vec<u8>, Vec<Vec<Vec<u8>>>)> {
        let bytes = self.db.export().into_iter().map(|(a, b, c)| {
            (a, b, c.into_iter().collect::<Vec<Vec<Vec<u8>>>>())
        }).collect();
        bytes
    }

    pub fn import(&self, export: Vec<(Vec<u8>, Vec<u8>, impl Iterator<Item = Vec<Vec<u8>>>)>)  {
        self.db.import(export)
    }

    /// Delete a named collection. This cannot be undone.
    pub fn delete_collection(&self, name: &str) -> Result<String, MauveError> {
        self.db.drop_tree(format!("mauve::{name}"))?;
        Ok(name.to_string())
    }

    /// Get backend status
    pub fn status(&self) -> Result<BackendState, MauveError> {
        Ok(self.clone().try_into()?)
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

#[derive(Clone, Serialize, Deserialize)]
pub struct MauveExport {
    pub data: Vec<(Vec<u8>, Vec<u8>, Vec<Vec<Vec<u8>>>)>,
}
