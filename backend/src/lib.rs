#[macro_use]
extern crate rocket;

use api::ApiDoc;
use backend::Backend;
use config::AppConfig;
use indexer::IndexerSignal;
use rocket::{
    fairing::{Fairing, Info, Kind},
    routes, Build, Orbit, Rocket,
};
use utoipa::OpenApi;
use utoipa_swagger_ui::SwaggerUi;

pub mod api;
pub mod backend;
pub mod collection;
pub mod config;
pub mod errors;
pub mod indexer;
pub mod labels;
pub mod meta;
pub mod objects;
pub mod search;
pub mod query;

pub fn mauve_rocket(config: AppConfig, backend: Backend) -> Rocket<Build> {
    let rocket = rocket::build()
        .configure(&config.rocket)
        .attach(ShtudownFairing {})
        .manage(config)
        .manage(backend)
        .mount(
            "/",
            SwaggerUi::new("/api/v1/<_..>").url("/api-docs/openapi.json", ApiDoc::openapi()),
        )
        .mount("/v1", routes![api::backend_status,])
        .mount("/v1/search", routes![api::search::search_collection])
        .mount(
            "/v1/objects/",
            routes! {
                api::objects::head_object,
                api::objects::get_object,
                api::objects::post_object,
                api::objects::put_object,
                api::objects::delete_object,
                api::objects::describe_object,
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

pub struct ShtudownFairing {}

#[rocket::async_trait]
impl Fairing for ShtudownFairing {
    fn info(&self) -> Info {
        Info {
            name: "Landing Thrusters",
            kind: Kind::Shutdown,
        }
    }

    async fn on_shutdown(&self, r: &Rocket<Orbit>) {
        log::info!("Hold your butts Ima firing the landing thrusters!");
        let be = match r.state::<Backend>() {
            Some(be) => be,
            None => {
                log::warn!("Couldn't find backend on shutdown!");
                return;
            }
        };
        match be.send_signal(IndexerSignal::Shutdown) {
            Ok(_) => (),
            Err(e) => log::error!("Failed to signal the backend worker {e}"),
        }
        log::info!("Failed to crash successful!")
    }
}
