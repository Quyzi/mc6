use std::collections::HashSet;

use crate::objects::ToFromMauve;
use crate::{errors::MauveError, labels::Label};
use macros::MauveObject;
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug, Default, MauveObject)]
pub struct Metadata {
    pub(crate) content_type: String,
    pub(crate) content_encoding: String,
    pub(crate) content_language: String,
    pub(crate) size: u64,
    pub(crate) labels: HashSet<Label>,
    pub(crate) offset_map: String,
}

impl Metadata {
    pub fn label_str(&self) -> String {
        let mut s = String::new();
        for label in &self.labels {
            s.push_str(&label.to_fwd());
            s.push(',');
        }
        s.trim_end_matches(',').to_string()
    }
}

pub struct ObjectWithMetadata {
    pub(crate) object: Vec<u8>,
    pub(crate) meta: Metadata,
}
