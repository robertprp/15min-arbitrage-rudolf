use std::{ops::Div, path::Path, str::FromStr};

use clap::{Parser, Subcommand};
use lib::AppResult;
use rust_decimal::{Decimal, MathematicalOps};
use service::{config::service::ConfigService, kalshi::service::KalshiService, market::Min15Symbols, polymarket::gamma::PolymarketGammaService, services::ServiceProvider};
use tokio::join;
use tracing::{info, warn};

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
    Unified,
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    pub async fn execute(self) -> AppResult<()> {
        let config = ConfigService::from_env()?;

        let services = ServiceProvider::new();
        services.add_service(config).await;

        let polymarket_gamma_service = services.get_service_unchecked::<PolymarketGammaService>().await;
        let kalshi_service = services.get_service_unchecked::<KalshiService>().await;
        match self.command {
            Commands::Unified => {
                let symbol = Min15Symbols::BTC;
                let poly_market = polymarket_gamma_service.get_current_15min_market_by_symbol(symbol.clone()).await?;
                info!("Polymarket market: {:?}", poly_market);
                
                let kalshi_market = kalshi_service.get_current_15min_market_by_symbol(symbol).await?;
                
                info!("Kalshi market: {:?}", kalshi_market);
                
                Ok(())
            }
        }
        
    }
}
