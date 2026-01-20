use std::str::FromStr;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::{Stream, StreamExt}; // Import StreamExt for map/select
use lib::AppResult;
use polymarket_client_sdk::{
    auth::{state::Authenticated, Credentials, Normal},
    clob::ws::{BookUpdate, Client, TradeMessage},
    types::{Address, B256, U256}, // Import ID types
};
use uuid::Uuid;

use crate::{
    config::service::ConfigService,
    services::{ServiceFactory, ServiceProvider},
};

/// 1. Define the Unified Enum
/// This allows the stream to yield a single type that wraps the specific updates.
#[derive(Debug)]
pub enum PolymarketFeedUpdate {
    Orderbook(BookUpdate),
    Trade(TradeMessage),
}

pub struct PolymarketWebSocketService {
    // Note: Kept generic bounds specific as per your snippet
    pub client: Client<Authenticated<Normal>>,
}

impl PolymarketWebSocketService {
    pub fn new(config_service: Arc<ConfigService>) -> AppResult<Self> {
        let poly_config = &config_service.polymarket;

        let api_key = Uuid::parse_str(&poly_config.api_key)
            .context("Failed to parse Polymarket API Key")?;

        let address = Address::from_str(&poly_config.funder)
            .context("Failed to parse Polymarket Funder Address")?;

        let credentials = Credentials::new(
            api_key,
            poly_config.api_secret.clone(),
            poly_config.api_passphrase.clone(),
        );

        let client = Client::default()
            .authenticate(credentials, address)
            .context("Failed to create authenticated Polymarket WS client")?;

        Ok(Self { client })
    }

    /// 2. Implement the Combined Subscription
    /// Accepts string arrays for ease of use, parses them, and merges the streams.
    pub fn subscribe_feed(
        &self,
        asset_ids: Vec<String>, // For Orderbook
        market_ids: Vec<String>, // For Trades
    ) -> AppResult<impl Stream<Item = AppResult<PolymarketFeedUpdate>>> {
        
        // --- A. Parse Inputs ---
        let assets_u256: Vec<U256> = asset_ids
            .into_iter()
            .map(|s| U256::from_str(&s).context("Invalid Asset ID format"))
            .collect::<Result<_, _>>()?;

        let markets_b256: Vec<B256> = market_ids
            .into_iter()
            .map(|s| B256::from_str(&s).context("Invalid Market ID format"))
            .collect::<Result<_, _>>()?;

        // --- B. Create Individual Streams ---
        
        // Stream 1: Orderbook (Market Channel)
        // We map the Result<BookUpdate> to Result<PolymarketFeedUpdate::Orderbook>
        let book_stream = self.client
            .subscribe_orderbook(assets_u256)
            .context("Failed to subscribe to orderbook")?
            .map(|res| res.map(PolymarketFeedUpdate::Orderbook).map_err(Into::into));

        // Stream 2: Trades (User Channel)
        // We map the Result<TradeMessage> to Result<PolymarketFeedUpdate::Trade>
        let trade_stream = self.client
            .subscribe_trades(markets_b256)
            .context("Failed to subscribe to trades")?
            .map(|res| res.map(PolymarketFeedUpdate::Trade).map_err(Into::into));

        // --- C. Merge Streams ---
        // futures::stream::select combines two streams into one.
        // The output stream will yield items whenever either source has data.
        let combined_stream = futures::stream::select(book_stream, trade_stream);

        Ok(combined_stream)
    }
}

#[async_trait]
impl ServiceFactory for PolymarketWebSocketService {
    async fn factory(services: ServiceProvider) -> AppResult<Self> {
        let config_service = services.get_service_unchecked::<ConfigService>().await;
        Self::new(config_service)
    }
}