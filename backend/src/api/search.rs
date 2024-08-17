use crate::{
    backend::Backend,
    search::{SearchRequest, SearchResponse},
};
use rocket::{http::Status, serde::json::Json, State};
use utoipa::{self as openapi};

#[openapi::path(
    tag = "search",
    context_path = "/v1/search",
    request_body = SearchRequest,
    responses(
        (status = 200, description = "Search completed", body = SearchResponse),
        (status = 500, description = "Server error"),
    )
)]
#[post("/", data = "<req>")]
pub async fn search_collection(
    req: Json<SearchRequest>,
    backend: &State<Backend>,
) -> Result<Json<SearchResponse>, (Status, String)> {
    match backend.perform_search(req.into_inner()).await {
        Ok(r) => Ok(Json(r)),
        Err(e) => Err((Status::InternalServerError, e.to_string())),
    }
}
