use std::{collections::HashSet, sync::Arc, time::Duration};

use dashmap::DashSet;

use super::*;
use crate::{
    backend::Backend,
    collection::Collection,
    errors::MauveError,
    objects::{ObjectRefs, ToFromMauve},
};

impl Backend {
    /// Perform a search against the backend
    pub async fn perform_search(&self, req: SearchRequest) -> Result<SearchResponse, MauveError> {
        let collection = self.get_collection(&req.collection)?;

        let includes = Arc::new(DashSet::new());
        let excludes = Arc::new(DashSet::new());

        for label in req.clone().labels {
            let collection = collection.clone();
            let (inc, exc) = (includes.clone(), excludes.clone());
            tokio::task::spawn(async move {
                let res = match &label {
                    SearchLabel::Include(inner) => collection.search_label(inner.clone(), inc),
                    SearchLabel::Exclude(inner) => collection.search_label(inner.clone(), exc),
                }
                .await;
                match res {
                    Ok(n) => log::debug!("query found {n} objects"),
                    Err(e) => log::error!("query error {e}"),
                }
            });
        }

        while Arc::strong_count(&includes) > 1 && Arc::strong_count(&excludes) > 1 {
            tokio::time::sleep(Duration::from_millis(200)).await
        }

        let mut results = HashSet::new();
        for item in includes.iter() {
            results.insert(item.clone());
        }
        results.retain(|item| !excludes.contains(item));

        let mut response = SearchResponse::new(req);

        let mut response_items = vec![];
        for object in results {
            let meta = collection.get_object_metadata(&object.name)?;
            response_items.push(FoundObject::new(object, meta));
        }
        response.set_ok(response_items);

        Ok(response)
    }
}

impl Collection {
    async fn search_label(
        &self,
        label: Label,
        target: Arc<DashSet<ObjectRef>>,
    ) -> Result<usize, MauveError> {
        match self.index_fwd().get(label.to_fwd().as_bytes()) {
            Ok(Some(bytes)) => {
                let objects = ObjectRefs::from_object(bytes.to_vec())?;
                let len = objects.len();
                for o in objects {
                    target.insert(o.clone());
                }
                Ok(len)
            }
            Ok(None) => Ok(0),
            Err(e) => Err(e.into()),
        }
    }
}
