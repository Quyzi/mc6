pub mod search;

use crate::{labels::Label, meta::Metadata, objects::ObjectRef};
use serde::{Deserialize, Serialize};
use thiserror::Error;
use utoipa::ToSchema;

#[derive(Error, Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum SearchError {
    #[error("Search has not been executed")]
    NotYetExecuted,
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub enum SearchLabel {
    Include(Label),
    Exclude(Label),
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchRequest {
    /// Name of the collection to search
    pub(crate) collection: String,

    /// Labels to apply to the search
    pub(crate) labels: Vec<SearchLabel>,
}

impl SearchRequest {
    pub fn new(c: &str) -> Self {
        Self {
            collection: c.to_string(),
            labels: vec![],
        }
    }

    pub fn include(&mut self, label: Label) {
        self.labels.push(SearchLabel::Include(label))
    }

    pub fn exclude(&mut self, label: Label) {
        self.labels.push(SearchLabel::Exclude(label))
    }

    pub fn includes(&mut self, labels: impl IntoIterator<Item = Label>) {
        for label in labels.into_iter() {
            self.include(label);
        }
    }

    pub fn excludes(&mut self, labels: impl IntoIterator<Item = Label>) {
        for label in labels.into_iter() {
            self.exclude(label)
        }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct FoundObject {
    pub object: ObjectRef,
    pub meta: Metadata,
}

impl FoundObject {
    pub fn new(object: ObjectRef, meta: Metadata) -> Self {
        Self { object, meta }
    }
}

#[derive(Clone, Debug, Serialize, Deserialize, ToSchema)]
pub struct SearchResponse {
    /// The search request
    pub req: SearchRequest,

    /// The result of the search
    pub result: Result<Vec<FoundObject>, SearchError>,
}

impl SearchResponse {
    pub fn new(req: SearchRequest) -> Self {
        Self {
            req,
            result: Err(SearchError::NotYetExecuted),
        }
    }

    pub fn set_ok(&mut self, objects: impl IntoIterator<Item = FoundObject>) {
        self.result = Ok(objects.into_iter().collect())
    }

    pub fn set_err(&mut self, e: SearchError) {
        self.result = Err(e)
    }
}
