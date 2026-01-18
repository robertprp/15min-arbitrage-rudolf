use std::{ops::Div, path::Path, str::FromStr};

use clap::{Parser, Subcommand};
use error_stack::report;
use lib::{AppResult, Error};
use rust_decimal::{Decimal, MathematicalOps};
use tokio::join;
use tracing::{info, warn};

pub const USDT_DECIMALS: u32 = 6;

#[derive(Parser)]
#[command(name = "rudolf")]
#[command(about = "Rudolf bot")]
#[command(version = "0.1.0")]
pub struct Cli {
    #[arg(short, long, default_value = ".env")]
    pub config: String,
    #[command(subcommand)]
    pub command: Commands,
}

#[derive(Subcommand)]
pub enum Commands {
    /// Start the arbitrage
    Arbitrage,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    pub async fn execute(self) -> AppResult<()> {
        let config = self.config;

        let path = Path::new(&config);
        let config = ConfigService::read_file(path)?;

        let services = ServiceProvider::new();
        services.add_service(config).await;

        match self.command {
            Commands::Check {
                polymarket_id,
                azuro_game_id,
            } => {
                tracing::info!("Starting manual arbitrage");
                tracing::info!("Polymarket NO ID: {}", polymarket_id);
                tracing::info!("Azuro YES ID: {}", azuro_game_id);
            }
        }
        
    }
}
