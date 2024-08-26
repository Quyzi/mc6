use clap::Parser;
use mc6_backend::{backend, config, errors::MauveError, mauve_rocket};
use simplelog::{CombinedLogger, TermLogger};
use std::path::PathBuf;

#[derive(Parser)]
#[command(version, about, long_about = None)]
struct CmdArgs {
    /// Config file to load
    #[arg(short, long, default_value = "mauve.yaml")]
    pub config_file: PathBuf,
}

#[tokio::main]
pub async fn main() -> Result<(), MauveError> {
    let args = CmdArgs::parse();
    CombinedLogger::init(vec![TermLogger::new(
        log::LevelFilter::Debug,
        simplelog::ConfigBuilder::new().build(),
        simplelog::TerminalMode::Mixed,
        simplelog::ColorChoice::Auto,
    )])
    .unwrap();

    log::info!("Mauve starting");

    let config = config::AppConfig::load(args.config_file)?;
    let backend = backend::Backend::open(config.clone())?;

    mauve_rocket(config, backend).launch().await?;

    Ok(())
}
