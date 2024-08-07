pub mod api;
pub mod backend;
pub mod collection;
pub mod config;
pub mod errors;

#[macro_use]
extern crate rocket;
use std::{path::PathBuf, str::FromStr};

use api::ApiDoc;
use errors::MauveError;
use simplelog::{CombinedLogger, TermLogger};
use utoipa::OpenApi;
use utoipa_scalar::{Scalar, Servable as ScalarServable};
use utoipa_swagger_ui::SwaggerUi;

#[tokio::main]
pub async fn main() -> Result<(), MauveError> {
    CombinedLogger::init(vec![TermLogger::new(
        log::LevelFilter::Debug,
        simplelog::ConfigBuilder::new()
            .set_time_offset_to_local()
            .unwrap()
            .build(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )])
    .unwrap();

    log::info!("Mauve starting");

    let config = config::AppConfig::load(PathBuf::from_str("mauve.yaml").unwrap())?;
    let backend = backend::Backend::open(config.clone())?;

    rocket::build()
        .configure(&config.rocket)
        .manage(config)
        .manage(backend)
        .mount("/", Scalar::with_url("/scalar", ApiDoc::openapi()))
        .mount(
            "/",
            SwaggerUi::new("/swagger-ui/<_..>").url("/api-docs/openapi.json", ApiDoc::openapi()),
        )
        .mount("/v1", routes![api::backend_status,])
        .mount(
            "/v1/objects/",
            routes! {
                api::objects::head_object,
                api::objects::get_object,
                api::objects::post_object,
                api::objects::put_object,
                api::objects::delete_object,
            },
        )
        .mount(
            "/v1/collections",
            routes![
                api::collections::list_collections,
                api::collections::list_objects,
            ],
        )
        .launch()
        .await?;

    Ok(())
}
