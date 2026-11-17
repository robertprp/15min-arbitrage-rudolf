use lib::AppResult;
use service::telemetry;
mod cli;
mod guard;
mod monitor;

#[tokio::main]
async fn main() -> AppResult<()> {
    // Create and start the application
    telemetry::init()?;
    // let app = AppRunner::new().await?;

    let cli = cli::Cli::parse_args();

    cli.execute().await
    // let app = AppRunner::new().await?;
    // app.start().await
}
