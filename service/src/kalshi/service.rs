use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use chrono::{Timelike, Utc};
use kalshi_rs::{events::models::GetEventResponse, Account, KalshiClient, KalshiWebsocketClient};
use lib::AppResult;
use chrono_tz::America::New_York;
use tracing::info; // Import the specific timezone

use crate::{config::service::ConfigService, market::Min15Symbols, services::{ServiceFactory, ServiceProvider}};

pub struct KalshiService {
    client: KalshiClient,
    ws_client: KalshiWebsocketClient
}

impl KalshiService {
    pub async fn new(config: Arc<ConfigService>) -> AppResult<Self> {
        let api_key_id = config.kalshi.api_key_id.clone();
        let pk_pem = config.kalshi.private_key_pem.clone();
        
        let account = Account::new(pk_pem.clone(), api_key_id.clone());
        
        let client = KalshiClient::new(account);
        
        let account = Account::new(pk_pem, api_key_id);
        
        let ws_client = KalshiWebsocketClient::new(account);
        
        ws_client.connect().await.context("Failed to connect to Kalshi WebSocket")?;
        
        Ok(Self { client, ws_client })
    }
    
    pub async fn get_current_15min_market_by_symbol(&self, symbol: Min15Symbols) -> AppResult<GetEventResponse> {
        let asset = symbol.to_string();
        let now_utc = Utc::now();
        let now_ny = now_utc.with_timezone(&New_York);
        
        let current_minute = now_ny.minute();
        let minutes_to_add = 15 - (current_minute % 15);
        
        // 2. Add the duration. This handles hour/day rollovers automatically.
        let target_time = now_ny
            .with_second(0).unwrap()
            .with_nanosecond(0).unwrap()
            .checked_add_signed(chrono::Duration::minutes(minutes_to_add as i64))
            .ok_or_else(|| anyhow::anyhow!("Time calculation overflow"))?;
        
        let date_part = target_time.format("%y").to_string(); 
        let day_time_part = target_time.format("%d%H%M").to_string();
        
        let month_part = target_time.format("%b").to_string().to_uppercase();
        let timestamp_str = format!("{}{}{}", date_part, month_part, day_time_part);
        let ticker = format!("KX{}15M-{}", asset, timestamp_str);
        
        info!("Kalshi ticker: {}", ticker);
        
        self.client.get_event(&ticker).await.context("Failed to get event")
    }
    
    async fn subscribe_orderbook(&self, ticker: String) -> AppResult<()> {
        let key = vec!["orderbook_delta"];
        let value = vec![ticker.as_str()];
        self.ws_client.subscribe(key, value).await.context("Failed to subscribe to Kalshi WebSocket")
    }
    
    async fn subscribe_fills(&self) -> AppResult<()> {
        let key = vec!["fill"];
        let value = vec![];
        self.ws_client.subscribe(key, value).await.context("Failed to subscribe to Kalshi WebSocket")
    }
}

#[async_trait]
impl ServiceFactory for KalshiService {
    async fn factory(services: ServiceProvider) -> AppResult<Self> {
        let config_service = services.get_service_unchecked::<ConfigService>().await;
        
        Ok(Self::new(config_service).await?)
    }
}
