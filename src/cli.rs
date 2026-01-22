use std::{ops::Div, path::Path, str::FromStr};

use chrono::{DateTime, Utc};
use clap::{Parser, Subcommand};
use lib::AppResult;
use rust_decimal::{Decimal, MathematicalOps, prelude::ToPrimitive};
use service::{chainlink::service::ChainLinkService, config::service::ConfigService, kalshi::service::KalshiService, market::{symbols::Min15Symbols}, polymarket::gamma::PolymarketGammaService, services::ServiceProvider};
use tokio::join;
use tracing::{info, warn};

use crate::monitor::{run_monitor, RunMonitorParams};


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
    Unified { symbol: Min15Symbols },
    /// Start the arbitrage
    Arbitrage {
        #[arg(long)]
        symbol: Min15Symbols,
        
        #[arg(long)]
        min_edge: f64,
    }
}

impl Cli {
    pub fn parse_args() -> Self {
        Self::parse()
    }

    pub async fn execute(self) -> AppResult<()> {
        let config = ConfigService::from_env()?;

        let services = ServiceProvider::new();
        services.add_service(config).await;

        // Warm up services
        let polymarket_gamma_service = services.get_service_unchecked::<PolymarketGammaService>().await;
        let kalshi_service = services.get_service_unchecked::<KalshiService>().await;
        let chainlink_service = services.get_service_unchecked::<ChainLinkService>().await;
        
        // Note: WebSocket service also needs to be initialized if not auto-created by provider
        services.warm_up::<service::polymarket::ws::PolymarketWebSocketService>().await;

        match self.command {
            Commands::Unified { symbol }=> {
                let poly_market = polymarket_gamma_service.get_current_15min_market_by_symbol(symbol.clone()).await?;
                let (open_price, open_timestamp) = chainlink_service.get_last_15min_open().await?;
                
                let kalshi_market = kalshi_service.get_current_15min_market_by_symbol(symbol).await?;
                
                Ok(())
            }
            // Execute the new command
            Commands::Arbitrage { min_edge, symbol } => {
                let params = RunMonitorParams {
                    services,
                    min_entry_edge_cents: min_edge,
                };
                
                run_monitor(params).await
            }
        }
        
    }
}