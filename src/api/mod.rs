pub mod collections;
pub mod objects;

use rocket::{serde::json::Json, State};
use utoipa::OpenApi;

use crate::{
    errors::MauveServeError,
    storage::{Backend, BackendState},
};

#[derive(OpenApi)]
#[openapi(
    info(description = "Mauve"),
    paths(
        objects::head_object,
        objects::get_object,
        objects::post_object,
        objects::put_object,
        objects::delete_object,
        collections::list_collections,
        collections::list_objects,
        collections::delete_collection,
        backend_status,
    )
)]
pub struct ApiDoc;

#[utoipa::path(
    tag = "Backend",
    context_path = "/v1",
    responses(
        (status = 200, description = "Backend status success", body = Json<BackendState>),
        (status = 500, description = "Server error")
    )
)]
#[get("/backend/status")]
/// Get backend status
pub fn backend_status(backend: &State<Backend>) -> Result<Json<BackendState>, MauveServeError> {
    Ok(Json(backend.status().map_err(|e| e.into())?))
}
