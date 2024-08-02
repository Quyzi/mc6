use std::fmt::{Debug, Display};

use rocket::http::Status;
use thiserror::Error;

pub type MauveServeError = (Status, String);

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

    #[error("{0}")]
    CollectionError(CollectionError),

    #[error("Oopsie {0}")]
    Oops(String),
}

impl From<rocket::Error> for MauveError {
    fn from(value: rocket::Error) -> Self {
        Self::RocketError(value.pretty_print().to_string())
    }
}

impl Into<MauveServeError> for MauveError {
    fn into(self) -> MauveServeError {
        match self {
            MauveError::ConfigError(err) => (Status::InternalServerError, err.to_string()),
            MauveError::RocketError(msg) => (Status::InternalServerError, msg),
            MauveError::SledError(err) => (Status::InternalServerError, err.to_string()),
            MauveError::CollectionError(err) => match err {
                CollectionError::PutObjectExistsNoReplace => (Status::Conflict, format!("{err}")),
                CollectionError::ObjectNotFound => (Status::NotFound, format!("{err}")),
            },
            MauveError::Oops(msg) => (Status::ImATeapot, msg),
            MauveError::Utf8Error(err) => (Status::InternalServerError, err.to_string()),
        }
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
