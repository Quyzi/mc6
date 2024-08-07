#[macro_use]
extern crate rocket;

use api::ApiDoc;
use backend::Backend;
use config::AppConfig;
use rocket::{routes, Build, Rocket};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub mod api;
pub mod backend;
pub mod collection;
pub mod config;
pub mod errors;

pub fn mauve_rocket(config: AppConfig, backend: Backend) -> Rocket<Build> {
    let rocket = rocket::build()
        .configure(&config.rocket)
        .manage(config)
        .manage(backend)
        .mount(
            "/",
            SwaggerUi::new("/api/v1/<_..>").url("/api-docs/openapi.json", ApiDoc::openapi()),
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
        );
    rocket
}
