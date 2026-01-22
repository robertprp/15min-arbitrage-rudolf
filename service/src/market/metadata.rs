use std::str::FromStr;
use anyhow::{bail, Context};
use kalshi_rs::events::models::GetEventResponse;
use lib::AppResult;
use polymarket_client_sdk::gamma::types::response::Market;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use tracing::info;

use crate::market::{exchange::Exchange, symbols::Min15Symbols};

#[derive(Debug, Clone)]
pub struct MarketMetadata {
    pub exchange: Exchange,
    pub ticker: Min15Symbols,
    pub strike_price: Decimal,
    pub last_updated_at: chrono::DateTime<chrono::Utc>,
    pub last_updated_timestamp_secs: i64,
}

impl MarketMetadata {
    /// Factory method to create a UnifiedMarket from Polymarket data
    pub fn from_polymarket(market: Market, strike_price_f64: f64) -> AppResult<Self> {
        let slug = market.slug
            .as_ref()
            .context("Polymarket slug is missing")?;

        // 2. Parse Ticker (logic abstracted below)
        let ticker = Self::parse_ticker_from_slug(slug)?;

        // 3. Convert Price
        let strike_price = Decimal::from_f64(strike_price_f64)
            .context("Failed to convert strike price to Decimal")?;

        Ok(Self {
            ticker,
            exchange: Exchange::Polymarket,
            strike_price,
            last_updated_at: chrono::Utc::now(),
            last_updated_timestamp_secs: chrono::Utc::now().timestamp(),
        })
    }

    /// Factory method to create a UnifiedMarket from Kalshi data
    pub fn from_kalshi(response: GetEventResponse) -> AppResult<Self> {
        let title = &response.event.title;
        let symbol_str = title.split_ascii_whitespace()
            .next()
            .context("Kalshi event title is empty")?;
        
        let ticker = Self::map_str_to_symbol(symbol_str)?;

        let market = match response.markets.first() {
            Some(market) => market,
            None => bail!("Kalshi market data is missing"),
        };

        let strike_price = match market.floor_strike {
            Some(strike) => Decimal::from_f64(strike).context("Failed to convert Kalshi strike price to Decimal")?,
            None => bail!("Kalshi market data is missing strike price"),
        };
        
        Ok(Self {
            ticker,
            exchange: Exchange::Kalshi,
            strike_price,
            last_updated_at: chrono::Utc::now(),
            last_updated_timestamp_secs: chrono::Utc::now().timestamp(),
        })
    }

    // --- Helper functions to clean up logic ---

    /// Helper to parse tickers specifically from Polymarket slugs (e.g., "btc-2024...")
    fn parse_ticker_from_slug(slug: &str) -> AppResult<Min15Symbols> {
        if slug.starts_with("btc") {
            Ok(Min15Symbols::BTC)
        } else if slug.starts_with("eth") {
            Ok(Min15Symbols::ETH)
        } else if slug.starts_with("sol") {
            Ok(Min15Symbols::SOL)
        } else {
            bail!("Unsupported Polymarket ticker in slug: {}", slug);
        }
    }

    /// specific helper to map raw strings to your Enum
    fn map_str_to_symbol(s: &str) -> AppResult<Min15Symbols> {
        match s.to_uppercase().as_str() {
            "BTC" => Ok(Min15Symbols::BTC),
            "ETH" => Ok(Min15Symbols::ETH),
            "SOL" => Ok(Min15Symbols::SOL),
            _ => bail!("Unsupported symbol: {}", s),
        }
    }
}