use anyhow::Context;
use lib::{AppResult};
use tracing::{Level, level_filters::LevelFilter, subscriber::set_global_default};
use tracing_subscriber::{filter::Targets, fmt::writer::MakeWriterExt, layer::SubscriberExt};

pub fn init() -> AppResult<()> {
    let log_level = LevelFilter::INFO;

    let stdout_log_layer = tracing_subscriber::fmt::layer()
        .compact()
        .with_ansi(true)
        .with_target(true)
        .with_writer(std::io::stdout.with_max_level(Level::INFO));

    let target = Targets::new()
        .with_target("rudolf", log_level)
        .with_target("service", log_level)
        .with_target("lib", log_level);

    let registry = tracing_subscriber::Registry::default()
        .with(target)
        .with(stdout_log_layer);

    // Initiazlie global subscriber
    set_global_default(registry).context("Failed to initialize global subscriber")?;
    // print_styled_logo();
    Ok(())
}