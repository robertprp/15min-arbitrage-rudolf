use async_trait::async_trait;
use lib::AppResult;
use polymarket_client_sdk::gamma::{types::{request::{EventByIdRequest, EventByIdRequestBuilder, EventsRequest, MarketByIdRequest, MarketBySlugRequest}, response::Market}, Client};

use crate::{market::Min15Symbols, services::{ServiceFactory, ServiceProvider}};

pub struct PolymarketGammaService {
    client: Client
}



impl PolymarketGammaService {
    pub fn new() -> Self {
        let client = Client::default();
        
        Self { client }
    }
    
    fn get_floored_15min_timestamp(timestamp: i64) -> i64 {
        const WINDOW_SECONDS: i64 = 15 * 60; // 900 seconds
        (timestamp / WINDOW_SECONDS) * WINDOW_SECONDS
    }
    
    pub async fn get_current_15min_market_by_symbol(&self, symbol: Min15Symbols) -> AppResult<Market> {
        let now = chrono::Utc::now();
        let current_ts = now.timestamp();
        let floored_ts = Self::get_floored_15min_timestamp(current_ts);
        let market_slug = format!("{}-updown-15m-{}", symbol.to_string().to_lowercase(), floored_ts);
        
        let market = self.client.market_by_slug(&MarketBySlugRequest::builder().slug(market_slug).build()).await?;
        
        Ok(market)
    }
}

#[async_trait]
impl ServiceFactory for PolymarketGammaService {
    async fn factory(_services: ServiceProvider) -> AppResult<Self> {
        Ok(Self::new())
    }
}
