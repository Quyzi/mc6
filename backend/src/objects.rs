use macros::MauveObject;
use serde::{Deserialize, Serialize};
use sled::IVec;
use std::{
    fmt::Display,
    ops::{Deref, DerefMut},
};

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

#[derive(Clone, Debug, Serialize, Deserialize, MauveObject)]
pub struct ObjectRefs(Vec<ObjectRef>);

impl ObjectRefs {
    pub fn new(inner: Vec<ObjectRef>) -> Self {
        Self(inner)
    }
}

impl IntoIterator for ObjectRefs {
    type Item = ObjectRef;

    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

impl DerefMut for ObjectRefs {
    fn deref_mut(&mut self) -> &mut Self::Target {
        &mut self.0
    }
}

impl Deref for ObjectRefs {
    type Target = Vec<ObjectRef>;
    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

pub trait ToFromMauve: Serialize + for<'de> Deserialize<'de> {
    fn to_object(&self) -> Result<Vec<u8>, MauveError>;
    fn from_object(b: Vec<u8>) -> Result<Self, MauveError>;
}

#[cfg(test)]
mod tests {
    use super::ToFromMauve;
    use crate::errors::MauveError;
    use macros::MauveObject;
    use rand::{thread_rng, Rng, RngCore};
    use serde::{Deserialize, Serialize};

    #[derive(Debug, Serialize, Deserialize, Clone, PartialEq, Eq, MauveObject)]
    struct TestObject {
        onekb: Vec<u8>,
        s: String,
        x: u128,
    }

    impl TestObject {
        fn rand() -> Self {
            let mut rng = thread_rng();
            let mut buf = [0u8; 1024];
            rng.fill_bytes(&mut buf);
            assert_ne!([0u8; 1024], buf);
            Self {
                onekb: buf.to_vec(),
                s: format!("{}", rng.gen_range(1..u128::MAX)),
                x: rng.gen(),
            }
        }
    }

    #[test]
    fn test_mauve_object() -> anyhow::Result<()> {
        for n in 0..10 {
            let object = TestObject::rand();
            let bytes = object.to_object()?;
            let got = TestObject::from_object(bytes)?;
            assert_eq!(object, got);
            println!("test {n} is ok");
        }
        Ok(())
    }
}
