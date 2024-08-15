//! Indexer
//!
//! The indexer owns its own Tx/Rx to receive Watch/Unwatch signals from the main thread.
//! When the backend opens a collection, whether it is new or existing, it sends a `Watch
//! (collection)` signal to the indexer.  The Indexer receives these signals and distributes
//! the signals to the appropriate task.  
//!
//! The job of the indexer is to manage indexer threads for each known collection. The indexer
//! thread watches their collection metadata for labels. The indexer thread maintains a
//! forward and reverse index of `Label => [ObjectRef, ...]`.

use crate::{
    backend::Backend, collection::Collection, errors::MauveError, meta::Metadata,
    objects::ObjectRef,
};
use dashmap::DashMap;
use flume::{Receiver, Sender};
use rocket::futures::{stream::FuturesUnordered, StreamExt};
use sled::{transaction::ConflictableTransactionError, Event};
use std::{fmt::Display, sync::Arc, time::Duration};

type CollectionName = String;

#[derive(Clone)]
pub enum IndexerSignal {
    Watch(Collection),
    Unwatch(Collection),
    Rebuild(Collection),
    Shutdown,
}

#[derive(Clone)]
pub struct Indexer {
    pub watching: Arc<DashMap<CollectionName, (Sender<IndexerSignal>, Receiver<IndexerSignal>)>>,
    pub mux: Arc<Vec<Sender<IndexerSignal>>>,
}

impl Indexer {
    pub fn initialize(backend: Backend) -> Result<Self, MauveError> {
        let watches = DashMap::new();
        let mut mux = vec![];

        for collection in backend.list_collections()? {
            log::info!(collection = collection; "Starting indexer for collection");
            // Create a channel for the indexer thread to control its children
            let (tx, rx) = flume::unbounded();
            mux.push(tx.clone());
            watches.insert(collection.clone(), (tx.clone(), rx.clone()));

            // Start a task thread for each known collection to maintain the index
            let backend = backend.clone();
            tokio::task::spawn(async move {
                let backend = backend;
                let chan = (tx.clone(), rx.clone());
                let collection = backend.get_collection(&collection)?;
                let indexer = CollectionIndexer::new(collection, chan);

                tokio::task::spawn(async move {
                    match indexer.run().await {
                        Ok(_) => log::info!("collection indexer exited"),
                        Err(e) => log::error!("collection indexer error {e}"),
                    }
                });

                Result::<(), MauveError>::Ok(())
            });
        }

        let this = Self {
            watching: Arc::new(watches),
            mux: Arc::new(mux),
        };

        Ok(this)
    }

    pub async fn run(
        &self,
        signals: (Sender<IndexerSignal>, Receiver<IndexerSignal>),
    ) -> Result<(), MauveError> {
        let (_tx, rx) = signals;
        for _ in rx.drain() {
            ()
        }
        let report = tokio::time::interval(Duration::from_secs(120));

        tokio::pin!(report);
        loop {
            tokio::select! {
                _ = report.tick() => {
                    let mut watching = String::new();
                    for watch in self.watching.iter() {
                        watching.push_str(&format!("{}, ", watch.key()));
                    }
                    let watching = watching.trim_end_matches(',');
                    log::info!("Indexer is alive, watching: {watching}");
                }
                Ok(sig) = rx.recv_async() => {
                    match sig {
                        IndexerSignal::Watch(c) => {
                            if !self.watching.contains_key(&c.name) {
                                let chan = flume::unbounded();
                                let indexer = CollectionIndexer::new(c.clone(), chan.clone());
                                let _ = self.watching.insert(c.name.clone(), chan);
                                tokio::task::spawn(async move {
                                    match indexer.clone().run().await {
                                        Ok(_) => Ok(()),
                                        Err(e) => {
                                            log::error!("error in collection indexer {indexer}: {e}");
                                            Err(e)
                                        }
                                    }
                                });
                            }
                        }
                        IndexerSignal::Unwatch(c) => {
                            match self.watching.get(&c.name) {
                                Some(entry) => {
                                    let (tx, _rx) = entry.value();
                                    tx.send(IndexerSignal::Unwatch(c))?;
                                },
                                None => (),
                            }
                        },
                        IndexerSignal::Shutdown => {
                            let mut futures = FuturesUnordered::new();
                            for tx in self.mux.iter() {
                                futures.push(tx.send_async(IndexerSignal::Shutdown));
                            }
                            while let Some(r) = futures.next().await {
                                match r {
                                    Ok(_) => (),
                                    Err(e) => log::error!("failed to shut down indexer {e}"),
                                }
                            }
                            return Ok(())
                        }
                        IndexerSignal::Rebuild(_c) => log::warn!("make rebuild work before you try it dumbass"),
                    }
                }
            }
        }
    }
}

#[derive(Clone)]
struct CollectionIndexer {
    pub(crate) collection: Collection,
    pub(crate) chan: (Sender<IndexerSignal>, Receiver<IndexerSignal>),
}

impl Display for CollectionIndexer {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{collection} {tx}/{rx}",
            collection = self.collection.name,
            tx = self.chan.0.len(),
            rx = self.chan.1.len(),
        )
    }
}

impl CollectionIndexer {
    pub fn new(
        collection: Collection,
        chan: (Sender<IndexerSignal>, Receiver<IndexerSignal>),
    ) -> Self {
        Self { collection, chan }
    }

    pub async fn run(self) -> Result<(), MauveError> {
        let meta = self.collection.data_tree();

        loop {
            tokio::select! {
                Some(event) = meta.watch_prefix(vec![]) => {
                    match self.process_event(event) {
                        Ok(_) => (),
                        Err(e) => log::error!("indexer failure {e}")
                    }
                },
                sig = self.chan.1.recv_async() => {
                    match sig {
                        Ok(sig) => match sig {
                            IndexerSignal::Unwatch(_) => break,
                            IndexerSignal::Rebuild(_) => (),
                            IndexerSignal::Shutdown => return Ok(()),
                            _ => (),
                        },
                        Err(e) => {
                            log::error!("indexer error {e}");
                            break
                        }
                    }
                }
            }
        }
        Ok(())
    }

    fn process_event(&self, event: Event) -> Result<(), MauveError> {
        match event {
            Event::Insert { key, value: _ } => {
                let object = String::from_utf8(key.to_vec())?;
                let or = ObjectRef::new(&self.collection.name, &object);
                let bytes = match self.collection.meta_tree().get(key)? {
                    Some(bytes) => bytes,
                    None => return Ok(()), // Skip if no metadata
                };
                let meta: Metadata = bincode::deserialize(&bytes.to_vec())?;

                for label in meta.labels {
                    self.upsert(self.collection.index_fwd(), label.to_fwd(), or.clone())?;
                    self.upsert(self.collection.index_rev(), label.to_rev(), or.clone())?;
                }
            }
            Event::Remove { key } => {
                let object = String::from_utf8(key.to_vec())?;
                let or = ObjectRef::new(&self.collection.name, &object);
                let bytes = match self.collection.meta_tree().remove(key)? {
                    Some(bytes) => bytes,
                    None => return Ok(()), // Skip if no metadata
                };
                let meta: Metadata = bincode::deserialize(&bytes.to_vec())?;
                for label in meta.labels {
                    self.downsert(self.collection.index_fwd(), label.to_fwd(), or.clone())?;
                    self.downsert(self.collection.index_rev(), label.to_rev(), or.clone())?;
                }
            }
        }
        Ok(())
    }

    /// Upsert a label into a target tree
    ///
    /// This inserts the objectref into the list with the given label.  
    /// This creates a new label if necessary.
    fn upsert(
        &self,
        target: sled::Tree,
        labelstr: String,
        or: ObjectRef,
    ) -> Result<(), MauveError> {
        target.transaction(|target| {
            match target.get(&labelstr)? {
                Some(old) => {
                    let mut old: Vec<ObjectRef> =
                        bincode::deserialize(&old.to_vec()).map_err(|e| {
                            ConflictableTransactionError::Storage(sled::Error::ReportableBug(
                                e.to_string(),
                            ))
                        })?;
                    old.push(or.clone());
                    let old = bincode::serialize(&old).map_err(|e| {
                        ConflictableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    let _ = target.insert(labelstr.clone().into_bytes(), old)?;
                }
                None => {
                    let new = bincode::serialize(&vec![or.clone()]).map_err(|e| {
                        ConflictableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    target.insert(labelstr.clone().into_bytes(), new)?;
                }
            }
            Ok(())
        })?;

        Ok(())
    }

    /// Downsert a label from an index tree
    ///
    /// This removes the ObjectRef from the list with the given label.  
    /// If removing the ref would leave an empty list, the label is removed.
    fn downsert(
        &self,
        target: sled::Tree,
        labelstr: String,
        or: ObjectRef,
    ) -> Result<(), MauveError> {
        target.transaction(|target| {
            match target.get(&labelstr)? {
                Some(old) => {
                    let mut old: Vec<ObjectRef> =
                        bincode::deserialize(&old.to_vec()).map_err(|e| {
                            ConflictableTransactionError::Storage(sled::Error::ReportableBug(
                                e.to_string(),
                            ))
                        })?;
                    if old.len() == 1 {
                        // short circuit remove unused label
                        let _ = target.remove(labelstr.clone().into_bytes())?;
                        return Ok(());
                    }
                    old.retain(|x| x != &or);
                    let old = bincode::serialize(&old).map_err(|e| {
                        ConflictableTransactionError::Storage(sled::Error::ReportableBug(
                            e.to_string(),
                        ))
                    })?;
                    let _ = target.insert(labelstr.clone().into_bytes(), old)?;
                }
                None => (),
            }
            Ok(())
        })?;

        Ok(())
    }
}
