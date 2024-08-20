use std::io::Cursor;

use crate::{
    backend::Backend,
    collection::Collection,
    config::AppConfig,
    errors::{MauveError, MauveServeError},
    meta::{Metadata, ObjectWithMetadata},
};
use rocket::{
    data::ToByteUnit,
    http::{Header, Status},
    response::Responder,
    Data, Request, Response, State,
};
use serde::{Deserialize, Serialize};
use utoipa::{self as openapi, ToSchema};

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
) -> Result<ObjectWithMetadata, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let object = collection.get_object(name).map_err(|e| e.into())?;
    let meta = collection.get_object_metadata(name).map_err(|e| e.into())?;
    Ok(ObjectWithMetadata { object, meta })
}

#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
        ("content-type" = String, Header, description = "Content Type"),
        ("content-encoding" = String, Header, description = "Content Encoding"),
        ("content-language" = String, Header, description = "Content Language"),
        ("x-mauve-labels" = String, Header, description = "Comma-separated key=value labels describing the object"),
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
pub async fn post_object(
    collection: &str,
    name: &str,
    payload: Data<'_>,
    meta: Metadata,
    backend: &State<Backend>,
    config: &State<AppConfig>,
) -> Result<String, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let payload = payload
        .open(config.mauve.object_max_size_mb.mebibytes())
        .into_bytes()
        .await
        .map_err(|e| (Status::InternalServerError, e.to_string()))?
        .to_vec();

    let result = collection
        .put_object(name, payload, false)
        .map_err(|e| e.into())?;

    let _ = collection
        .put_object_metadata(name, meta)
        .map_err(|e| e.into())?;

    Ok(result.to_string())
}

#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
        ("content-type" = String, Header, description = "Content Type"),
        ("content-encoding" = String, Header, description = "Content Encoding"),
        ("content-language" = String, Header, description = "Content Language"),
        ("x-mauve-labels" = String, Header, description = "Comma-separated key=value labels describing the object"),
    ),
    request_body = Vec<u8>,
    responses(
        (status = 200, description = "Object upserted into collection", body = String),
        (status = 500, description = "Server error"),
    )
)]
/// Put an object into a collection. If the object already exists, the old will be overwritten.
#[put("/<collection>/<name>", data = "<payload>")]
pub async fn put_object(
    collection: &str,
    name: &str,
    payload: Data<'_>,
    meta: Metadata,
    backend: &State<Backend>,
    config: &State<AppConfig>,
) -> Result<String, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let payload = payload
        .open(config.mauve.object_max_size_mb.mebibytes())
        .into_bytes()
        .await
        .map_err(|e| (Status::InternalServerError, e.to_string()))?
        .to_vec();

    let result = collection
        .put_object(name, payload, true)
        .map_err(|e| e.into())?;

    let _ = collection
        .put_object_metadata(name, meta)
        .map_err(|e| e.into())?;

    Ok(result.to_string())
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

#[derive(Clone, Serialize, Deserialize, ToSchema)]
pub struct DescribeResponse {
    pub collection: String,
    pub name: String,
    pub meta: Metadata,
}

impl DescribeResponse {
    pub fn new(collection: &Collection, name: &str) -> Result<Self, MauveServeError> {
        let meta = collection.get_object_metadata(name).map_err(|e| e.into())?;

        Ok(Self {
            name: name.to_string(),
            collection: collection.name.clone(),
            meta,
        })
    }
}

#[rocket::async_trait]
impl<'r> Responder<'r, 'static> for DescribeResponse {
    fn respond_to(self, _req: &'r Request<'_>) -> rocket::response::Result<'static> {
        let output = serde_json::to_string(&self);
        if output.is_err() {
            Response::build()
                .status(Status::InternalServerError)
                .streamed_body(Cursor::new(
                    output.err().unwrap().to_string().as_bytes().to_vec(),
                ))
                .ok()
        } else {
            let json = output.unwrap();
            let labelstr = self.meta.label_str();
            Response::build()
                .status(Status::Ok)
                .header(Header::new("content-type", "application/json"))
                .header(Header::new("x-mauve-content-type", self.meta.content_type))
                .header(Header::new(
                    "x-mauve-content-encoding",
                    self.meta.content_encoding,
                ))
                .header(Header::new(
                    "x-mauve-content-language",
                    self.meta.content_language,
                ))
                .header(Header::new("x-mauve-labels", labelstr))
                .header(Header::new(
                    "x-mauve-offsets-inclusive",
                    self.meta.offset_map,
                ))
                .streamed_body(Cursor::new(json.as_bytes().to_vec()))
                .ok()
        }
    }
}

#[openapi::path(
    tag = "objects",
    context_path = "/v1/objects",
    params(
        ("collection" = String, description = "Name of the collection"),
        ("name" = String, description = "Name of the object"),
    ),
    responses(
        (status = 200, description = "Object described", body = DescribeResponse),
        (status = 404, description = "Object not found"),
        (status = 500, description = "Server error"),
    )
)]
/// Describe an object
#[get("/describe/<collection>/<name>")]
pub fn describe_object(
    collection: &str,
    name: &str,
    backend: &State<Backend>,
) -> Result<DescribeResponse, MauveServeError> {
    let collection = backend.get_collection(collection).map_err(|e| e.into())?;
    let response = DescribeResponse::new(&collection, name)?;
    Ok(response)
}
