use std::sync::Arc;

use async_trait::async_trait;
use futures::Stream;
use lib::AppResult;
use polymarket_client_sdk::{rtds::{ChainlinkPrice, Client}, ws::config::Config};

use crate::{config::service::ConfigService, services::{ServiceFactory, ServiceProvider}};

pub struct ChainLinkService {
    client: Client,
}

impl ChainLinkService {
    pub fn new() -> Self {
        let client = Client::default();
        Self { client }
    }
    
    
    // asset in form of btc/usd, eth/usd
    pub fn stream_chainlink_prices(&self, asset: String) -> impl Stream<Item = Result<ChainlinkPrice, polymarket_client_sdk::error::Error>> {
        let result = self.client.subscribe_chainlink_prices(Some(asset)).unwrap();
        
        result
    }
}

#[async_trait]
impl ServiceFactory for ChainLinkService {
    async fn factory(_services: ServiceProvider) -> AppResult<Self> {
        Ok(Self::new())
    }
}
