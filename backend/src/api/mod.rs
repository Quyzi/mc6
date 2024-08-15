pub mod collections;
pub mod objects;

use crate::api::objects::DescribeResponse;
use crate::labels::Label;
use crate::meta::Metadata;
use crate::{
    backend::{Backend, BackendState, TreeState},
    errors::MauveServeError,
};
use rocket::{serde::json::Json, State};
use utoipa::OpenApi;

#[derive(OpenApi)]
#[openapi(
    info(description = "Mauve"),
    paths(
        objects::head_object,
        objects::get_object,
        objects::post_object,
        objects::put_object,
        objects::delete_object,
        objects::describe_object,
        collections::list_collections,
        collections::list_objects,
        collections::delete_collection,
        backend_status,
    ),
    components(schemas(BackendState, TreeState, DescribeResponse, Metadata, Label),)
)]
pub struct ApiDoc;

#[utoipa::path(
    tag = "Backend",
    context_path = "/v1",
    responses(
        (status = 200, description = "Backend status success", body = BackendState),
        (status = 500, description = "Server error")
    )
)]
#[get("/backend/status")]
/// Get backend status
pub fn backend_status(backend: &State<Backend>) -> Result<Json<BackendState>, MauveServeError> {
    Ok(Json(backend.status().map_err(|e| e.into())?))
}
