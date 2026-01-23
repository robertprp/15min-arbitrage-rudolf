use async_trait::async_trait;
use chrono::{Datelike, Timelike};
use chrono_tz::America::New_York;
use lib::AppResult;
use polymarket_client_sdk::gamma::{
    Client,
    types::{request::MarketBySlugRequest, response::Market},
};

use crate::{
    market::symbols::{Hour1Symbols, Min15Symbols},
    services::{ServiceFactory, ServiceProvider},
    utils::get_floored_15min_timestamp,
};

pub struct PolymarketGammaService {
    client: Client,
}

impl Default for PolymarketGammaService {
    fn default() -> Self {
        Self::new()
    }
}

impl PolymarketGammaService {
    pub fn new() -> Self {
        let client = Client::default();

        Self { client }
    }

    pub fn get_slug_by_symbol(&self, symbol: Min15Symbols) -> String {
        let now = chrono::Utc::now();
        let current_ts = now.timestamp();
        let floored_ts = get_floored_15min_timestamp(current_ts);
        let value = format! {"{:?}", symbol};
        let market_slug = format!("{}-updown-15m-{}", value.to_lowercase(), floored_ts);

        market_slug
    }

    pub async fn get_current_15min_market_by_symbol(
        &self,
        symbol: Min15Symbols,
    ) -> AppResult<Market> {
        let market_slug = self.get_slug_by_symbol(symbol);

        let market = self
            .client
            .market_by_slug(&MarketBySlugRequest::builder().slug(market_slug).build())
            .await?;

        Ok(market)
    }

    pub async fn get_current_1h_market_by_symbol(&self, symbol: Hour1Symbols) -> AppResult<Market> {
        // Format: bitcoin-up-or-down-january-20-4pm-et

        let now = chrono::Utc::now().with_timezone(&New_York);

        // Target next full hour
        let target_time = now
            .with_minute(0)
            .unwrap()
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            .checked_add_signed(chrono::Duration::hours(1))
            .ok_or_else(|| anyhow::anyhow!("Time calculation overflow"))?;

        let asset_str = match symbol {
            Hour1Symbols::BTC => "bitcoin",
            Hour1Symbols::ETH => "ethereum",
        };

        // Full month name (lowercase), Day, Hour (12h format), AM/PM (lowercase)
        let month = target_time.format("%B").to_string().to_lowercase();
        let day = target_time.day();
        let hour_12 = target_time.format("%l").to_string().trim().to_string(); // %l is space padded
        let am_pm = target_time.format("%P").to_string();

        let market_slug = format!(
            "{}-up-or-down-{}-{}-{}{}-et",
            asset_str, month, day, hour_12, am_pm
        );

        self.get_market_by_slug(&market_slug).await
    }

    pub async fn get_market_by_slug(&self, slug: &str) -> AppResult<Market> {
        let market = self
            .client
            .market_by_slug(&MarketBySlugRequest::builder().slug(slug).build())
            .await?;

        Ok(market)
    }
}

#[async_trait]
impl ServiceFactory for PolymarketGammaService {
    async fn factory(_services: ServiceProvider) -> AppResult<Self> {
        Ok(Self::new())
    }
}
