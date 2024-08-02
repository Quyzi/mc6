use crate::{
    errors::{MauveError, MauveServeError},
    storage::Backend,
};
use rocket::{http::Status, State};
use utoipa as openapi;

/// Check if an object exists in a collection.
#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
    ),
    responses(
        (status = 200, description = "Object exists"),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Server error"),
    ),
)]
#[head("/<collection>/<name>")]
pub fn head_object(
    collection: &str,
    name: &str,
    backend: &State<Backend>,
) -> Result<Status, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    match collection.head_object(name).map_err(|e| e.into())? {
        true => Ok(Status::Ok),
        false => Ok(Status::NotFound),
    }
}

/// Get an object from a collection.
#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
    ),
    responses(
        (status = 200, description = "Object found", body = Vec<u8>),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Server error"),
    )
)]
#[get("/<collection>/<name>")]
pub fn get_object(
    collection: &str,
    name: &str,
    backend: &State<Backend>,
) -> Result<Vec<u8>, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let object = collection.get_object(name).map_err(|e| e.into())?;
    Ok(object)
}

#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
    ),
    request_body = Vec<u8>,
    responses(
        (status = 200, description = "Object inserted into collection", body = String),
        (status = 409, description = "Object already exists"),
        (status = 500, description = "Server error"),
    )
)]
/// Post an object into a collection. If the object already exists, this will return an error.
/// To replace the object, use PUT instead.
#[post("/<collection>/<name>", data = "<payload>")]
pub fn post_object(
    collection: &str,
    name: &str,
    payload: Vec<u8>,
    backend: &State<Backend>,
) -> Result<String, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let result = collection
        .put_object(name, payload, false)
        .map_err(|e| e.into())?;
    Ok(result)
}

#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
    ),
    request_body = Vec<u8>,
    responses(
        (status = 200, description = "Object upserted into collection", body = String),
        (status = 500, description = "Server error"),
    )
)]
/// Put an object into a collection. If the object already exists, the old will be overwritten.
#[put("/<collection>/<name>", data = "<payload>")]
pub fn put_object(
    collection: &str,
    name: &str,
    payload: Vec<u8>,
    backend: &State<Backend>,
) -> Result<String, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let result = collection
        .put_object(name, payload, true)
        .map_err(|e| e.into())?;
    Ok(result)
}

#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
    ),
    responses(
        (status = 200, description = "Object deleted successfully", body = Option<Vec<u8>>),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Server error"),
    )
)]
/// Delete an object from a collection. If the object existed, it is removed and the object is returned.
#[delete("/<collection>/<name>")]
pub fn delete_object(
    collection: &str,
    name: &str,
    backend: &State<Backend>,
) -> Result<Option<Vec<u8>>, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let deleted = collection.delete_object(&name).map_err(|e| e.into())?;
    match deleted {
        Some(bytes) => Ok(Some(bytes)),
        None => {
            Err(MauveError::CollectionError(crate::errors::CollectionError::ObjectNotFound).into())
        }
    }
}
