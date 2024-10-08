use serde::{Deserialize, Serialize};
use std::str::FromStr;

use crate::{
    errors::{CollectionError::ObjectNotFound, MauveError},
    labels::Label,
    meta::Metadata,
    objects::{ObjectRef, ToFromMauve},
};

#[derive(Clone)]
pub struct Collection {
    pub name: String,
    pub(crate) data: sled::Tree,
    pub(crate) meta: sled::Tree,
    pub(crate) index_fwd: sled::Tree,
    pub(crate) index_rev: sled::Tree,
}

impl Collection {
    pub(crate) fn data_tree(&self) -> sled::Tree {
        self.data.clone()
    }

    pub(crate) fn meta_tree(&self) -> sled::Tree {
        self.meta.clone()
    }

    pub(crate) fn index_fwd(&self) -> sled::Tree {
        self.index_fwd.clone()
    }

    pub(crate) fn index_rev(&self) -> sled::Tree {
        self.index_rev.clone()
    }

    /// Get a list of object keys being stored in the collection matching a given prefix.
    /// This iterates over every object stored. This can be very expensive and time consuming
    /// if there are a huge number of objects stored. Use with caution
    pub fn list_objects(
        &self,
        prefix: &str,
    ) -> Result<impl IntoIterator<Item = String>, MauveError> {
        Ok(self.data.scan_prefix(prefix)
            .filter_map(|result| {
                let k = match result {
                    Ok((k, _)) => k,
                    Err(e) => {
                        log::error!(err = e.to_string(); "collection key error");
                        return None
                    }
                };
                match String::from_utf8(k.to_vec()) {
                    Ok(s) => Some(s),
                    Err(e) => {
                        log::error!(err = e.to_string(); "collection key failed to deserialize to string");
                        None
                    }
                }
            }))
    }

    /// Check if an object exists in the collection.
    pub fn head_object(&self, ident: &str) -> Result<bool, MauveError> {
        Ok(self.data.contains_key(ident)?)
    }

    /// Get a `T: ToFromMauve` from the collection
    pub fn get_object_t<T: ToFromMauve>(&self, ident: &str) -> Result<T, MauveError>
    where
        T: Serialize + for<'de> Deserialize<'de>,
    {
        let bytes = self.get_object(ident)?;
        Ok(T::from_object(bytes)?)
    }

    /// Get an object as bytes by its name.
    ///
    /// **Note:** `get_object_t` should be used in almost all cases.
    ///
    pub fn get_object(&self, ident: &str) -> Result<Vec<u8>, MauveError> {
        match self.data.get(ident) {
            Ok(Some(bytes)) => Ok(bytes.to_vec()),
            Ok(None) => Err(MauveError::CollectionError(ObjectNotFound)),
            Err(e) => {
                log::error!(err = e.to_string(); "get object failed to get object");
                Err(MauveError::SledError(e))
            }
        }
    }

    /// Get all metadata for a given object in this collection.
    pub fn get_object_metadata(&self, ident: &str) -> Result<Metadata, MauveError> {
        match self.meta.get(ident) {
            Ok(Some(bytes)) => {
                let meta = Metadata::from_object(bytes.to_vec())?;
                Ok(meta)
            }
            Ok(None) => Err(MauveError::CollectionError(ObjectNotFound)),
            Err(e) => {
                log::error!(err = e.to_string(); "get object metadata failed");
                Err(MauveError::SledError(e))
            }
        }
    }

    /// Put an object into the collection with the given identity.
    ///
    /// **Note:** `put_object_t` should be used in almost all cases.
    ///
    /// If an object already exists with that identity and the replace flag is true, the old object will
    /// be replaced with the new. The old object will *not* be returned.
    ///
    /// If an object already exists with that identity and the replace flag is false, an error is returned.
    pub fn put_object(
        &self,
        ident: &str,
        object: Vec<u8>,
        replace: bool,
    ) -> Result<ObjectRef, MauveError> {
        let old = self.data.get(ident)?;
        match old {
            Some(_) => {
                log::debug!(ident = ident, replace = replace; "object already exists with ident");
                if !replace {
                    return Err(MauveError::CollectionError(
                        crate::errors::CollectionError::PutObjectExistsNoReplace,
                    ));
                }
            }
            None => (),
        }

        self.data.insert(ident, object)?;
        Ok(ObjectRef::new(&self.name, ident))
    }

    /// Put a `T: ToFromMauve` into the collection with the given identity.
    ///
    /// If an object already exists with that identity and the replace flag is true, the old object will
    /// be replaced with the new. The old object will *not* be returned.
    ///
    /// If an object already exists with that identity and the replace flag is false, an error is returned.
    pub fn put_object_t<T: ToFromMauve>(
        &self,
        ident: &str,
        object: &T,
        replace: bool,
    ) -> Result<ObjectRef, MauveError> {
        let bytes = object.to_object()?;
        self.put_object(ident, bytes, replace)?;
        Ok(ObjectRef::new(&self.name, ident))
    }

    /// Insert metadata about an object, replacing the existing.
    pub fn put_object_metadata(&self, ident: &str, meta: Metadata) -> Result<String, MauveError> {
        let meta_bytes = meta.to_object()?;
        match self.meta.insert(ident, meta_bytes) {
            Ok(Some(_old)) => {
                log::debug!(ident = ident; "Replaced existing object metadata with {meta:?}")
            }
            Ok(None) => (),
            Err(e) => {
                log::error!(ident = ident, err = e.to_string(); "failed to delete object");
                return Err(MauveError::SledError(e));
            }
        }
        Ok(ident.to_string())
    }

    /// Delete an object by its name. This returns the object if one existed.
    /// Deleting an object that does not exist is a no-op.
    pub fn delete_object_t<T: ToFromMauve>(&self, ident: &str) -> Result<Option<T>, MauveError> {
        match self.delete_object(ident)? {
            Some(bytes) => Ok(Some(T::from_object(bytes)?)),
            None => Ok(None),
        }
    }

    /// Delete an object by its name. This returns the object if one existed.
    /// Deleting an object that does not exist is a no-op.
    ///
    /// **Note:** `delete_object_t` should be used in almost all cases.
    pub fn delete_object(&self, ident: &str) -> Result<Option<Vec<u8>>, MauveError> {
        let old = self.data.remove(ident)?;
        match old {
            Some(old) => Ok(Some(old.to_vec())),
            None => Ok(None),
        }
    }

    /// Delete metadata about an object.
    pub fn delete_metadata(&self, ident: &str) -> Result<Option<Metadata>, MauveError> {
        let old = self.meta.remove(ident)?;
        match old {
            Some(bytes) => {
                let val = Metadata::from_object(bytes.to_vec())?;
                Ok(Some(val))
            }
            None => Ok(None),
        }
    }

    /// List all labels known to this collection.
    pub fn list_labels(&self) -> Result<impl IntoIterator<Item = Label>, MauveError> {
        let mut labels = vec![];
        for label in self.index_fwd.into_iter() {
            let (label, _) = label?;
            let label = String::from_utf8(label.to_vec())?;
            labels.push(Label::from_str(&label)?);
        }
        Ok(labels)
    }
}
