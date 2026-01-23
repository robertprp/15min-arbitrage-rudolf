use std::str::FromStr;

use anyhow::{Context, anyhow};
use async_trait::async_trait;
use chrono::DateTime;
use futures::Stream;
use lib::AppResult;
use polymarket_client_sdk::rtds::{ChainlinkPrice, Client};
use reqwest::Client as HttpClient;
use rust_decimal::Decimal;

use crate::{
    chainlink::types::{CHAINLINK_BTC_PRICE_FEED_ID, ChainlinkResponse, ToChainlinkAsset},
    market::symbols::Min15Symbols,
    services::{ServiceFactory, ServiceProvider},
};

pub struct ChainLinkService {
    client: Client,
}

impl Default for ChainLinkService {
    fn default() -> Self {
        Self::new()
    }
}

impl ChainLinkService {
    pub fn new() -> Self {
        let client = Client::default();
        Self { client }
    }

    // asset in form of btc/usd, eth/usd
    pub fn stream_chainlink_prices(
        &self,
        asset: impl ToChainlinkAsset,
    ) -> impl Stream<Item = Result<ChainlinkPrice, polymarket_client_sdk::error::Error>> {
        self.client
            .subscribe_chainlink_prices(Some(asset.to_chainlink_asset().to_string()))
            .unwrap()
    }

    /// Fetches the last 15-minute candle open price from Chainlink Data Streams.
    /// Returns (Price, UnixTimestamp).
    pub async fn get_last_15min_open(symbol: Min15Symbols) -> AppResult<(i64, Decimal)> {
        if symbol != Min15Symbols::BTC {
            return Err(anyhow!("Unsupported symbol for Chainlink API"));
        }

        let url = format!(
            "https://data.chain.link/api/historical-timescale-stream-data?feedId={}&timeRange=1D",
            CHAINLINK_BTC_PRICE_FEED_ID
        );

        let http_client = HttpClient::new();
        let response = http_client
            .get(&url)
            .send()
            .await
            .context("Failed to send request to Chainlink API")?
            .json::<ChainlinkResponse>()
            .await
            .context("Failed to deserialize Chainlink API response")?;

        let last_node = response
            .data
            .mercury_history
            .nodes
            .last()
            .ok_or_else(|| anyhow!("Chainlink API returned no data nodes"))?;

        // 1. Parse Timestamp
        // Format: "2026-01-20T22:00:00+00:00"
        let dt = DateTime::parse_from_rfc3339(&last_node.time_bucket)
            .context("Failed to parse Chainlink timestamp")?;
        let timestamp = dt.timestamp();

        // 2. Parse Price
        // Value comes in as 18 decimals (Wei), e.g., "90176452620396700000000"
        let raw_open =
            Decimal::from_str(&last_node.open).context("Failed to parse open price decimal")?;

        // Normalize: Divide by 10^18 to get USD value
        let scale = Decimal::from(1_000_000_000_000_000_000_u64);
        let price = raw_open / scale;

        Ok((timestamp, price))
    }
}

#[async_trait]
impl ServiceFactory for ChainLinkService {
    async fn factory(_services: ServiceProvider) -> AppResult<Self> {
        Ok(Self::new())
    }
}
