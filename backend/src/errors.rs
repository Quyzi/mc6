use std::fmt::{Debug, Display};

use sled::transaction::ConflictableTransactionError;
use thiserror::Error;

use crate::indexer::IndexerSignal;

#[derive(Clone, Debug, Error)]
pub enum MauveError {
    #[error("Config error {0}")]
    ConfigError(#[from] figment::Error),

    #[error("Rocket exploded {0}")]
    RocketError(String),

    #[error("Utf8 encoding error: {0}")]
    Utf8Error(#[from] std::string::FromUtf8Error),

    #[error("Sled error {0}")]
    SledError(#[from] sled::Error),

    #[error("Sled tx error {0}")]
    SledTxError(#[from] sled::transaction::TransactionError),

    #[error("IO error {0}")]
    IoError(String),

    #[error("Signaling error {0}")]
    SignalError(#[from] flume::SendError<IndexerSignal>),

    #[error("Invalid label string {0}")]
    InvalidLabel(String),

    #[error("{0}")]
    CollectionError(CollectionError),

    #[error("bincode failed {0}")]
    BincodeError(String),

    #[error("cbor serde {0}")]
    CborError(String),

    #[error("Oopsie {0}")]
    Oops(String),
}

impl From<std::io::Error> for MauveError {
    fn from(value: std::io::Error) -> Self {
        MauveError::IoError(value.to_string())
    }
}

impl From<ciborium::de::Error<std::io::Error>> for MauveError {
    fn from(value: ciborium::de::Error<std::io::Error>) -> Self {
        Self::CborError(value.to_string())
    }
}

impl From<ciborium::ser::Error<std::io::Error>> for MauveError {
    fn from(value: ciborium::ser::Error<std::io::Error>) -> Self {
        Self::CborError(value.to_string())
    }
}

impl Into<ConflictableTransactionError> for MauveError {
    fn into(self) -> ConflictableTransactionError {
        ConflictableTransactionError::Abort(sled::Error::ReportableBug(self.to_string()))
    }
}

#[derive(Clone)]
pub enum CollectionError {
    PutObjectExistsNoReplace,
    ObjectNotFound,
}

impl Debug for CollectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{self}")
    }
}

impl Display for CollectionError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            CollectionError::PutObjectExistsNoReplace => {
                write!(f, "Object exists with ident, replace=false")
            }
            CollectionError::ObjectNotFound => write!(f, "Object not found"),
        }
    }
}
