use crate::{backend::Backend, errors::MauveServeError};
use rocket::{serde::json::Json, State};
use utoipa as openapi;

#[openapi::path(
    tag = "collections",
    context_path = "/v1/collections",
    responses(
        (status = 200, description = "List collections successful", body = Vec<String>),
        (status = 500, description = "Server error"),
    )
)]
#[get("/")]
/// Get a list of collections
pub fn list_collections(backend: &State<Backend>) -> Result<Json<Vec<String>>, MauveServeError> {
    let mut collections = vec![];
    for collection in backend.list_collections().map_err(|e| e.into())? {
        collections.push(collection);
    }
    Ok(Json(collections))
}

#[openapi::path(
    tag = "collections",
    context_path = "/v1/collections",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("prefix" = String, Query, description = "Object prefix to query")
    ),
    responses(
        (status = 200, description = "List objects successful", body = Vec<String>),
        (status = 500, description = "Server error"),
    )
)]
#[get("/<collection>?<prefix>")]
/// List objects in a collection
pub fn list_objects(
    collection: &str,
    prefix: &str,
    backend: &State<Backend>,
) -> Result<Json<Vec<String>>, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let objects = collection
        .list_objects(prefix)
        .map_err(|e| e.into())?
        .into_iter()
        .collect();
    Ok(Json(objects))
}

#[openapi::path(
    tag = "collections",
    context_path = "/v1/collections",
    params(
        ("collection" = String, Path, description = "Name of the collection"),
    ),
    responses(
        (status = 200, description = "Collection deleted successfully", body = String),
        (status = 500, description = "Server error"),
    )
)]
/// Delete a collection
#[delete("/<collection>")]
pub fn delete_collection(
    collection: &str,
    backend: &State<Backend>,
) -> Result<String, MauveServeError> {
    backend
        .delete_collection(collection)
        .map_err(|e| e.into())?;
    Ok(collection.to_string())
}
