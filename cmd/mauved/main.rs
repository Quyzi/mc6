use std::{path::PathBuf, str::FromStr};

use mc6_backend::{backend, config, errors::MauveError, mauve_rocket};
use simplelog::{CombinedLogger, TermLogger};

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

    mauve_rocket(config, backend).launch().await?;

    Ok(())
}
