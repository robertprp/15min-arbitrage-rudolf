use anyhow::{Context, bail};
use kalshi_rs::events::models::GetEventResponse;
use lib::AppResult;
use polymarket_client_sdk::gamma::types::response::Market;
use rust_decimal::{Decimal, prelude::FromPrimitive};

use crate::{
    chainlink::service::ChainLinkService,
    kalshi::service::KalshiService,
    market::{
        exchange::{KalshiExchange, PolymarketExchange},
        symbols::Min15Symbols,
    },
};

#[derive(Debug, Clone)]
pub struct MarketMetadata<Exchange> {
    pub symbol: Min15Symbols,
    pub strike_price: Decimal,

    _phantom: std::marker::PhantomData<Exchange>,
}

impl MarketMetadata<PolymarketExchange> {
    pub async fn new_poly(market: Market) -> AppResult<Self> {
        let slug = market.slug.as_ref().context("Polymarket slug is missing")?;

        // 2. Parse Ticker (logic abstracted below)
        let symbol = Self::parse_ticker_from_slug(slug)?;

        let (_ts, strike_price) = ChainLinkService::get_last_15min_open(symbol.clone()).await?;

        Ok(Self {
            symbol,
            strike_price,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl MarketMetadata<KalshiExchange> {
    pub async fn new_kalshi(market: GetEventResponse) -> AppResult<Self> {
        let title = &market.event.title;
        let symbol_str = title
            .split_ascii_whitespace()
            .next()
            .context("Kalshi event title is empty")?;

        let symbol = Self::map_str_to_symbol(symbol_str)?;

        let market = match market.markets.first() {
            Some(market) => market,
            None => bail!("Kalshi market data is missing"),
        };

        let strike_price = match market.floor_strike {
            Some(strike) => Decimal::from_f64(strike)
                .context("Failed to convert Kalshi strike price to Decimal")?,
            None => {
                let (_ts, strike_price) = KalshiService::get_strike_price(symbol.clone()).await?;
                strike_price
            }
        };

        Ok(Self {
            symbol,
            strike_price,
            _phantom: std::marker::PhantomData,
        })
    }
}

impl<T> MarketMetadata<T> {
    /// Helper to parse tickers specifically from Polymarket slugs (e.g., "btc-2024...")
    fn parse_ticker_from_slug(slug: &str) -> AppResult<Min15Symbols> {
        if slug.starts_with("btc") {
            Ok(Min15Symbols::BTC)
        } else if slug.starts_with("eth") {
            Ok(Min15Symbols::ETH)
        } else {
            bail!("Unsupported Polymarket ticker in slug: {}", slug);
        }
    }

    /// specific helper to map raw strings to your Enum
    fn map_str_to_symbol(s: &str) -> AppResult<Min15Symbols> {
        match s.to_uppercase().as_str() {
            "BTC" => Ok(Min15Symbols::BTC),
            "ETH" => Ok(Min15Symbols::ETH),
            _ => bail!("Unsupported symbol: {}", s),
        }
    }
}
