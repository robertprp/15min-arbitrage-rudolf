use std::str::FromStr;

use anyhow::{bail, Context};
use kalshi_rs::events::models::GetEventResponse;
use lib::AppResult;
use polymarket_client_sdk::gamma::types::response::Market;
use rust_decimal::{prelude::FromPrimitive, Decimal};

#[derive(Debug, Clone)]
pub struct UnifiedMarket {
    pub ticker: Min15Symbols,
    pub exchange: Exchange,
    pub strike_price: Decimal,
}

#[derive(Debug, Clone)]
pub enum Exchange {
    Kalshi,
    Polymarket
}

#[derive(Debug, Clone)]
pub enum Min15Symbols {
    BTC,
    ETH,
    SOL,
    // XRP,
}

#[derive(Debug, Clone)]
pub enum Hour1Symbols {
    BTC,
    ETH,
}

impl ToString for Min15Symbols {
    fn to_string(&self) -> String {
        match self {
            Min15Symbols::BTC => "BTC".to_string(),
            Min15Symbols::ETH => "ETH".to_string(),
            Min15Symbols::SOL => "SOL".to_string(),
            // Min15Symbols::XRP => "XRP".to_string(),
        }
    }
}

impl ToString for Hour1Symbols {
    fn to_string(&self) -> String {
        match self {
            Hour1Symbols::BTC => "BTC".to_string(),
            Hour1Symbols::ETH => "ETH".to_string(),
        }
    }
}

// Market and strike price
impl TryFrom<(Market, f64)> for UnifiedMarket {
    type Error = anyhow::Error;
    
    fn try_from((market, strike_price): (Market, f64)) -> AppResult<UnifiedMarket> {
        // 1. Extract Symbol from the slug (e.g., "btc-up-or-down...")
        // We look at the first part of the slug string.
        let slug = market.slug
            .as_ref()
            .context("Market slug is missing")?;
        
        let ticker = if slug.starts_with("btc") {
            Min15Symbols::BTC
        } else if slug.starts_with("eth") {
            Min15Symbols::ETH
        } else if slug.starts_with("sol") {
            Min15Symbols::SOL
        } else {
            bail!("Unsupported ticker in slug: {}", slug);
        };
        
        let exchange = Exchange::Polymarket;
        let strike_price = Decimal::from_f64(strike_price).context("Failed to convert strike price to Decimal")?;
        
        Ok(UnifiedMarket {
            ticker,
            exchange,
            strike_price,
        })
    }
}

impl TryFrom<GetEventResponse> for UnifiedMarket {
    type Error = anyhow::Error;
    fn try_from(response: GetEventResponse) -> AppResult<Self> {
        let s = response.event.title.split_ascii_whitespace().next();
        
        if s.is_none() {
            bail!("Unsupported symbol")
        }
        
        let ticker = match s.unwrap() {
            "BTC" => Min15Symbols::BTC,
            "ETH" => Min15Symbols::ETH,
            "SOL" => Min15Symbols::SOL,
            _ => bail!("Unsupported symbol"),
        };
        
        let strike_price = response.event.strike_date;
        
        if strike_price.is_none() {
            bail!("Missing strike price")
        }
        
        let unified_market = UnifiedMarket {
            ticker,
            exchange: Exchange::Kalshi,
            strike_price: Decimal::from_str(&strike_price.unwrap())?
        };
        
        Ok(unified_market)
    }
}