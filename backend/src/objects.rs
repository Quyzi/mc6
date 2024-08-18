use std::fmt::Display;

use serde::{Deserialize, Serialize};
use sled::IVec;

use crate::errors::MauveError;

#[derive(Clone, Debug, Eq, PartialEq, PartialOrd, Ord, Hash, Default, Serialize, Deserialize)]
pub struct ObjectRef {
    pub collection: String,
    pub name: String,
}

impl ObjectRef {
    pub fn new(collection: &str, name: &str) -> Self {
        Self {
            collection: collection.to_ascii_lowercase(),
            name: name.to_ascii_lowercase(),
        }
    }
}

impl Display for ObjectRef {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{}/{}", self.collection, self.name)
    }
}

impl TryFrom<(IVec, IVec)> for ObjectRef {
    type Error = MauveError;

    fn try_from((collection, name): (IVec, IVec)) -> Result<Self, Self::Error> {
        let collection = String::from_utf8(collection.to_vec())?;
        let name = String::from_utf8(name.to_vec())?;
        Ok(Self { name, collection })
    }
}
