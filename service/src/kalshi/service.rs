use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use chrono::{Timelike, Utc};
use kalshi_rs::{events::models::GetEventResponse, Account, KalshiClient};
use lib::AppResult;
use chrono_tz::America::New_York; // Import the specific timezone

use crate::{config::service::ConfigService, market::Min15Symbols, services::{ServiceFactory, ServiceProvider}};

pub struct KalshiService {
    client: KalshiClient
}

impl KalshiService {
    pub fn new(config: Arc<ConfigService>) -> Self {
        let api_key_id = config.kalshi.api_key_id.clone();
        let pk_pem = config.kalshi.private_key_pem.clone();
        
        let account = Account::new(pk_pem, api_key_id);
        
        let client = KalshiClient::new(account);
        
        Self { client }
    }
    
    pub async fn get_current_15min_market_by_symbol(&self, symbol: Min15Symbols) -> AppResult<GetEventResponse> {
        let asset = symbol.to_string();
        let now_utc = Utc::now();
        let now_ny = now_utc.with_timezone(&New_York);
        
        let current_minute = now_ny.minute();
        let rounded_minute = current_minute - (current_minute % 15);
        
        let target_time = now_ny
            .with_minute(rounded_minute).unwrap()
            .with_second(0).unwrap();

        let date_part = target_time.format("%y").to_string(); // "26"
        let day_time_part = target_time.format("%d%H%M").to_string(); // "181500"
        
        let month_part = target_time.format("%b").to_string().to_uppercase();
        let timestamp_str = format!("{}{}{}", date_part, month_part, day_time_part);
        let ticker = format!("KM{}15MIN-{}", asset, timestamp_str);
        
        self.client.get_event(&ticker).await.context("Failed to get event")
    }
}

#[async_trait]
impl ServiceFactory for KalshiService {
    async fn factory(services: ServiceProvider) -> AppResult<Self> {
        let config_service = services.get_service_unchecked::<ConfigService>().await;
        
        Ok(Self::new(config_service))
    }
}
