use std::{collections::HashMap, io::Cursor};

use rocket::{
    http::{Header, Status},
    outcome::Outcome,
    request::FromRequest,
    response::Responder,
    Request, Response,
};
use serde::{Deserialize, Serialize};

use crate::errors::MauveError;

#[derive(Serialize, Deserialize, Clone, Debug, Default)]
pub struct Metadata {
    pub(crate) content_type: String,
    pub(crate) content_encoding: String,
    pub(crate) content_language: String,
    pub(crate) size: u64,
    pub(crate) labels: HashMap<String, String>,
}

#[rocket::async_trait]
impl<'r> FromRequest<'r> for Metadata {
    /// The associated error to be returned if derivation fails.
    type Error = MauveError;

    /// Derives an instance of `Self` from the incoming request metadata.
    ///
    /// If the derivation is successful, an outcome of `Success` is returned. If
    /// the derivation fails in an unrecoverable fashion, `Error` is returned.
    /// `Forward` is returned to indicate that the request should be forwarded
    /// to other matching routes, if any.
    async fn from_request(
        request: &'r Request<'_>,
    ) -> Outcome<Self, (Status, Self::Error), Status> {
        match Metadata::try_from(request) {
            Ok(meta) => Outcome::Success(meta),
            Err(e) => {
                log::error!(err = e.to_string(); "failed to create metadata for request");
                Outcome::Error((Status::InternalServerError, e))
            }
        }
    }
}

impl<'a> TryFrom<&rocket::Request<'a>> for Metadata {
    type Error = MauveError;

    fn try_from(req: &rocket::Request) -> Result<Self, Self::Error> {
        let mut this = Self::default();
        let uri = req.uri().to_string();
        log::debug!("{uri}");

        for header in req.headers().iter() {
            match header.name().as_str().to_uppercase().as_str() {
                "CONTENT-TYPE" => this.content_type = header.value().to_string(),
                "CONTENT-ENCODING" => this.content_encoding = header.value().to_string(),
                "CONTENT-LANGUAGE" => this.content_language = header.value().to_string(),
                "CONTENT-LENGTH" => {
                    let size: u64 = match header.value().parse() {
                        Ok(s) => s,
                        Err(e) => {
                            log::error!(err = e.to_string(); "error parsing content-length as u64");
                            0
                        }
                    };
                    this.size = size;
                }
                "X-MAUVE-LABELS" => {
                    let labels = header.value();
                    for label in labels.split(',') {
                        let (name, value) = match label.split_once('=') {
                            Some((k, v)) => (k, v),
                            None => continue,
                        };
                        this.labels.insert(name.to_string(), value.to_string());
                    }
                }
                _ => continue,
            }
        }
        Ok(this)
    }
}

pub struct ObjectWithMetadata {
    pub(crate) object: Vec<u8>,
    pub(crate) meta: Metadata,
}

impl<'r> Responder<'r, 'static> for ObjectWithMetadata {
    fn respond_to(self, _req: &'r Request<'_>) -> rocket::response::Result<'static> {
        log::debug!("{meta:?}", meta = self.meta);
        let mut labels = String::new();
        for (k, v) in self.meta.labels {
            labels.push_str(&format!("{k}={v},"));
        }
        labels = labels.trim_end_matches(',').to_string();

        Response::build()
            .header(Header::new("Content-Type", self.meta.content_type))
            .header(Header::new("Content-Encoding", self.meta.content_encoding))
            .header(Header::new("Content-Language", self.meta.content_language))
            .header(Header::new("x-mauve-labels", labels))
            .sized_body(self.object.len(), Cursor::new(self.object))
            .ok()
    }
}
