
use std::str::FromStr;
use std::sync::Arc;
use anyhow::Context;
use async_trait::async_trait;
use futures::{Stream, StreamExt, TryStreamExt}; 
use lib::AppResult;
use polymarket_client_sdk::{
    auth::{state::Authenticated, Credentials, Normal},
    clob::ws::{BestBidAsk, Client, OrderMessage, TradeMessage},
    types::{Address, B256, U256}, 
};
use uuid::Uuid;
use buildstructor::Builder;

use crate::{
    config::service::ConfigService,
    services::{ServiceFactory, ServiceProvider},
};

#[derive(Debug)]
pub enum PolymarketFeedUpdate {
    BestBidAsk(BestBidAsk),
    Trade(TradeMessage),
}

pub struct PolymarketWebSocketService {
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

    pub fn subscribe_feed(
        &self,
        asset_ids: Vec<U256>, 
        market_ids: Vec<B256>, 
    ) -> AppResult<impl Stream<Item = AppResult<PolymarketFeedUpdate>>> {
        
        // 1. Market Data (BBA)
        let bba_stream = self.client
            .subscribe_best_bid_ask(asset_ids)
            .context("Failed to subscribe to market BestBidAsk")?
            .map(|res| res.map(PolymarketFeedUpdate::BestBidAsk)).map_err(Into::into);
        
        // 2. Market Data (Trades - Optional, good for volume tracking)
        let trade_stream = self.client
            .subscribe_trades(market_ids.clone())
            .context("Failed to subscribe to trades")?
            .map(|res| res.map(PolymarketFeedUpdate::Trade).map_err(Into::into));

        // Merge Streams
        let combined_stream = futures::stream::select(bba_stream, trade_stream);

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