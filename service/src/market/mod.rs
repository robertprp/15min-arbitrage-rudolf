use std::str::FromStr;

use anyhow::bail;
use kalshi_rs::events::models::GetEventResponse;
use lib::AppResult;
use polymarket_client_sdk::gamma::types::response::Market;
use rust_decimal::Decimal;

pub struct UnifiedMarket {
    pub ticker: Min15Symbols,
    pub exchange: Exchange,
    pub strike_price: Decimal,
}

pub enum Exchange {
    Kalshi,
    Polymarket
}

pub enum Min15Symbols {
    BTC,
    ETH,
    SOL,
    // XRP,
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

impl TryFrom<Market> for UnifiedMarket {
    type Error = anyhow::Error;
    
    fn try_from(market: Market) -> Self {
        UnifiedMarket {
            ticker: match market.symbol.as_str() {
                "BTC" => Min15Symbols::BTC,
                "ETH" => Min15Symbols::ETH,
                "SOL" => Min15Symbols::SOL,
                _ => panic!("Unsupported symbol"),
            },
            exchange: Exchange::Polymarket,
            strike_price: market.strike_price,
        }
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
