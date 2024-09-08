use std::{path::PathBuf, str::FromStr};

use figment::{
    providers::{Format, Serialized, Yaml},
    Figment,
};
use serde::{Deserialize, Serialize};

use crate::errors::MauveError;

#[derive(Serialize, Deserialize, Clone, Default, Debug)]
pub struct AppConfig {
    pub rocket: rocket::Config,
    pub sled: SledConfig,
    pub mauve: MauveConfig,
}

impl AppConfig {
    pub fn load(file: PathBuf) -> Result<Self, MauveError> {
        Ok(Figment::from(Serialized::defaults(Self::default()))
            .merge(Yaml::file(file))
            .extract()?)
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct MauveConfig {
    pub object_max_size_mb: u64,
    pub query_concurrency: u16,
    pub query_timeout_secs: u64,
}

impl Default for MauveConfig {
    fn default() -> Self {
        Self {
            object_max_size_mb: 30,
            query_concurrency: 16,
            query_timeout_secs: 60,
        }
    }
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub struct SledConfig {
    pub cache_capacity: u64,
    pub flush_every_ms: Option<u64>,
    pub path: PathBuf,
    pub mode: String,
    pub use_compression: bool,
    pub compression_factor: i32,
    pub idgen_persist_interval: u64,
}

impl Default for SledConfig {
    fn default() -> Self {
        Self {
            cache_capacity: 1024 * 1024 * 1024,
            flush_every_ms: Some(500),
            path: PathBuf::from_str("data/").unwrap(),
            mode: "HighThroughput".to_string(),
            use_compression: false,
            compression_factor: 5,
            idgen_persist_interval: 1_000_000,
        }
    }
}

impl Into<sled::Config> for SledConfig {
    fn into(self) -> sled::Config {
        sled::Config::new()
            .cache_capacity(self.cache_capacity)
            .flush_every_ms(self.flush_every_ms)
            .path(self.path)
            .mode(match self.mode.as_str() {
                "HighThroughput" => sled::Mode::HighThroughput,
                "LowSpace" => sled::Mode::LowSpace,
                _ => sled::Mode::HighThroughput,
            })
            .use_compression(self.use_compression)
            .compression_factor(self.compression_factor)
            .idgen_persist_interval(self.idgen_persist_interval)
    }
}
