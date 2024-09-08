use std::{collections::BTreeSet, future::IntoFuture, sync::Arc, time::Duration};
use dashmap::DashSet;
use futures_util::{stream::FuturesUnordered, StreamExt};
use thiserror::Error;
use tokio::time::{timeout, Timeout};

use crate::{backend::Backend, objects::ObjectRef};

#[derive(Debug, Error, Clone)]
pub enum QueryError {
    #[error("Query timed out after {0} seconds")]
    Timeout(u64),

    #[error("Cannot search for label with no name or value")]
    Derp(),
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub enum QueryOp {
    Include,
    Exclude,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub struct QueryField {
    op: QueryOp,
    collection: String,
    name: Option<String>,
    value: Option<String>,
}

impl QueryField {
    pub fn new(op: QueryOp, collection: String, name: Option<String>, value: Option<String>) -> Self {
        Self {
            op, collection, name, value
        }
    }

    pub fn include(collection: &str, name: &str, value: &str) -> Self {
        let name = if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        };
        
        let value = if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        };

        Self::new(QueryOp::Include, collection.to_string(), name, value)
    }

    pub fn exclude(collection: &str, name: &str, value: &str) -> Self {
        let name = if name.is_empty() {
            None
        } else {
            Some(name.to_string())
        };
        
        let value = if value.is_empty() {
            None
        } else {
            Some(value.to_string())
        };

        Self::new(QueryOp::Exclude, collection.to_string(), name, value)
    }

    pub(crate) fn run(&self, parent: &QueryRequest) -> (Self, Result<Vec<ObjectRef>, QueryError>) {
        let res = match (&self.name, &self.value) {
            (Some(_), Some(_)) => self.lookup(parent),
            (Some(_), None) => self.prefix(parent),
            (None, Some(_)) => self.suffix(parent),
            _ => Err(QueryError::Derp())
        };
        (self.clone(), res)
    }

    fn lookup(&self, parent: &QueryRequest) -> Result<Vec<ObjectRef>, QueryError> {
        todo!()
    }

    fn prefix(&self, parent: &QueryRequest) -> Result<Vec<ObjectRef>, QueryError> {
        todo!()
    }

    fn suffix(&self, parent:&QueryRequest) -> Result<Vec<ObjectRef>, QueryError> {
        todo!()
    }
}

#[derive(Clone)]
pub struct QueryRequest {
    pub(crate) backend: Backend,
    pub(crate) fields: Vec<QueryField>,
    pub(crate) timeout_secs: u64
}

impl QueryRequest {
    pub fn new(backend: Backend) -> Self {
        let timeout_secs = backend.get_config().mauve.query_timeout_secs;
        Self {
            backend,
            fields: vec![],
            timeout_secs,
        }
    }

    pub fn new_with_timeout(backend: Backend, timeout_secs: u64) -> Self {
        Self {
            backend, 
            fields: vec![],
            timeout_secs, 
        }
    }

    pub fn append_field(&mut self, f: QueryField) {
        self.fields.push(f)
    }

    pub async fn run(&self) -> Result<QueryResult, QueryError> {
        match timeout(Duration::from_secs(self.timeout_secs), self.clone().run_inner()).await {
            Ok(res) => res,
            Err(_) => Err(QueryError::Timeout(self.timeout_secs)),
        }
    }

    async fn run_inner(&self) -> Result<QueryResult, QueryError> {
        let config = self.backend.get_config();
        let mut results = BTreeSet::new();
        let mut excludes = BTreeSet::new();
        let mut errors = vec![];
        let fields = self.fields.clone();
        let mut i = fields.iter();
        let mut futures = FuturesUnordered::new();

        while futures.len() < config.mauve.query_concurrency as usize {
            if let Some(field) = i.next() {
                let f = tokio::task::spawn(async move {field.run(&self)});
                futures.push(f);
            }
        }

        while let Some((field, res)) = futures.next().await {
            match res {
                Ok(mut yay) => {
                    match field.op {
                        QueryOp::Include => {
                            yay.retain(|this| !excludes.contains(this));
                            results.extend(yay)
                        },
                        QueryOp::Exclude => {
                            results.retain(|this| !yay.contains(this));
                            excludes.extend(yay);
                        },
                    }
                },
                Err(e) => errors.push((field.clone(), e))
            }
            if let Some(field) = i.next() {
                let f = tokio::task::spawn(async move {field.run(&self)});
                futures.push(f);
            }
        }

        results.retain(|this| !excludes.contains(this));

        Ok(QueryResult {
            req: self.clone(),
            results: results.into_iter().collect(),
            errors,
        })
    }

}

#[derive(Clone)]
pub struct QueryResult {
    pub req: QueryRequest,
    pub results: Vec<ObjectRef>,
    pub errors: Vec<(QueryField, QueryError)>,
}
