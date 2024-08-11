pub mod log_store;
pub mod state_machine;

use std::{io::Cursor, marker::PhantomData};

use bytes::Bytes;
use openraft::{impls::OneshotResponder, RaftTypeConfig, TokioRuntime};
use serde::{Deserialize, Serialize};

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Request {
    DeleteCollection {
        name: String,
    },
    PutObject {
        collection: String,
        name: String,
        object: Bytes,
    },
    DeleteObject {
        collection: String,
        name: String,
    },
}

#[derive(Serialize, Deserialize, Clone, Debug)]
pub enum Response {
    DeleteCollection {},
    PutObject {},
    DeleteObject {},
}

#[derive(Clone, Copy, Debug, Default, PartialOrd, Ord, PartialEq, Eq, Serialize, Deserialize)]
pub struct TypeConfig {}
impl openraft::RaftTypeConfig for TypeConfig {
    type D = Request;
    type R = Response;
    type NodeId = u64;
    type Node = openraft::BasicNode;
    type Entry = openraft::Entry<TypeConfig>;
    type SnapshotData = Cursor<Vec<u8>>;
    type AsyncRuntime = TokioRuntime;
    type Responder = OneshotResponder<TypeConfig>;
}

pub struct Raft<C: RaftTypeConfig> {
    _ghost: PhantomData<C>,
}
