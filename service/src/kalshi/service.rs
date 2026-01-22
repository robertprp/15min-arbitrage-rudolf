use std::sync::Arc;

use anyhow::{Context, bail};
use async_trait::async_trait;
use chrono::{Timelike, Utc};
use chrono_tz::America::New_York;
use kalshi_rs::{
    Account, KalshiClient, KalshiWebsocketClient,
    events::models::GetEventResponse,
    markets::models::GetMarketOrderbookResponse,
    portfolio::models::{CreateOrderRequest, CreateOrderResponse},
};
use lib::AppResult;
use rust_decimal::{Decimal, prelude::FromPrimitive};
use serde_json::Value;
use tracing::info;

use crate::{
    config::service::ConfigService,
    market::symbols::{Hour1Symbols, Min15Symbols},
    services::{ServiceFactory, ServiceProvider},
    strategy::Order,
};

pub struct KalshiService {
    client: KalshiClient,
    pub ws_client: KalshiWebsocketClient,
}

impl KalshiService {
    pub async fn new(config: Arc<ConfigService>) -> AppResult<Self> {
        let api_key_id = config.kalshi.api_key_id.clone();
        let pk_pem = config.kalshi.private_key_pem.clone();

        let account = Account::new(pk_pem.clone(), api_key_id.clone());

        let client = KalshiClient::new(account);

        let account = Account::new(pk_pem, api_key_id);

        let ws_client = KalshiWebsocketClient::new(account);

        ws_client
            .connect()
            .await
            .context("Failed to connect to Kalshi WebSocket")?;

        Ok(Self { client, ws_client })
    }

    pub async fn get_last_traded_price(&self, symbol: Min15Symbols) -> AppResult<(i64, Decimal)> {
        let symbol = format!("{:?}", symbol);
        let url = format!(
            "https://kalshi-public-docs.s3.amazonaws.com/external/crypto/{}_current.json?allowRequestEvenIfPageIsHidden=true",
            symbol.to_lowercase()
        );
        let response = reqwest::get(&url).await?;
        let data: serde_json::Value = response.json().await?;

        let ts_ms = data["maturity_ts_ms"]
            .as_i64()
            .context("Invalid maturity_ts_ms data")?;
        let value = data["candlesticks"]["15M"]["close"]
            .as_f64()
            .context("Invalid close value")?;

        let ts_secs = ts_ms / 1000;

        let price = Decimal::from_f64(value).context("Failed to convert value to Decimal")?;

        Ok((ts_secs, price))
    }

    pub async fn place_market_order(
        &self,
        ticker: String,
        side: String,   // yes or no
        action: String, // buy or sell
        order: Order,
    ) -> AppResult<CreateOrderResponse> {
        let contracts = order.shares;
        let price_cents = order.price_cents;
        let mut req = CreateOrderRequest {
            ticker,
            action: action.clone(),
            side: side.clone(),
            count: contracts.into(),
            type_: Some(String::from("market")),
            buy_max_cost: None,
            cancel_order_on_pause: None,
            client_order_id: None,
            expiration_ts: None,
            no_price: None,
            no_price_dollars: None,
            order_group_id: None,
            post_only: Some(false),
            reduce_only: None,
            self_trade_prevention_type: None,
            time_in_force: Some(String::from("good_till_canceled")),
            yes_price: None,
            yes_price_dollars: None,
        };

        if side.eq("yes") {
            req.yes_price = Some(price_cents.into());
        } else {
            req.no_price = Some(price_cents.into());
        }

        if (action.eq("buy")) {
            // TODO fix this bc maybe price_cents is less than 3.
            let cents = 2;
            if side.eq("yes") {
                // Reduce few cents yes_price to make sure we are filled
                req.yes_price = Some((price_cents + cents).into());
            } else {
                // Reduce few cents no_price to make sure we are filled
                req.no_price = Some((price_cents + cents).into());
            }
        }

        self.client
            .create_order(&req)
            .await
            .context("Failed to create kalshi order")
    }

    pub async fn get_current_15min_market_by_symbol(
        &self,
        symbol: Min15Symbols,
    ) -> AppResult<GetEventResponse> {
        let asset = format!("{:?}", symbol);
        let now_utc = Utc::now();
        let now_ny = now_utc.with_timezone(&New_York);

        let current_minute = now_ny.minute();
        let minutes_to_add = 15 - (current_minute % 15);

        let target_time = now_ny
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            .checked_add_signed(chrono::Duration::minutes(minutes_to_add as i64))
            .ok_or_else(|| anyhow::anyhow!("Time calculation overflow"))?;

        let date_part = target_time.format("%y").to_string();
        let day_time_part = target_time.format("%d%H%M").to_string();

        let month_part = target_time.format("%b").to_string().to_uppercase();
        let timestamp_str = format!("{}{}{}", date_part, month_part, day_time_part);
        let ticker = format!("KX{}15M-{}", asset, timestamp_str);

        info!("Kalshi ticker {}", &ticker);
        self.get_event(&ticker).await
    }

    pub async fn get_event(&self, ticker: &str) -> AppResult<GetEventResponse> {
        self.client
            .get_event(ticker)
            .await
            .context("Failed to get event")
    }

    pub async fn get_current_1h_market_by_symbol(
        &self,
        symbol: Hour1Symbols,
    ) -> AppResult<GetEventResponse> {
        let asset = format!("{:?}", symbol);
        let now_utc = Utc::now();
        let now_ny = now_utc.with_timezone(&New_York);

        let current_minute = now_ny.minute();
        // Calculate minutes to add to reach the next full hour
        let minutes_to_add = 60 - (current_minute % 60);

        let target_time = now_ny
            .with_second(0)
            .unwrap()
            .with_nanosecond(0)
            .unwrap()
            .checked_add_signed(chrono::Duration::minutes(minutes_to_add as i64))
            .ok_or_else(|| anyhow::anyhow!("Time calculation overflow"))?;

        let date_part = target_time.format("%y").to_string();
        let day_time_part = target_time.format("%d%H%M").to_string();

        let month_part = target_time.format("%b").to_string().to_uppercase();
        let timestamp_str = format!("{}{}{}", date_part, month_part, day_time_part);
        // Ticker format: KXBTC1H-26JAN201500
        let ticker = format!("KX{}1H-{}", asset, timestamp_str);

        info!("Kalshi 1H ticker: {}", ticker);

        self.client
            .get_event(&ticker)
            .await
            .context("Failed to get 1H event")
    }

    pub async fn subscribe_orderbook(&self, market_ticker: String) -> AppResult<()> {
        let channels = vec!["orderbook_delta"];

        let markets = vec![market_ticker.as_str()];

        self.ws_client
            .subscribe(channels, markets)
            .await
            .context("Failed to subscribe to Kalshi WebSocket")
    }

    pub async fn subscribe_fills(&self) -> AppResult<()> {
        let key = vec!["fill"];
        let value = vec![];
        self.ws_client
            .subscribe(key, value)
            .await
            .context("Failed to subscribe to Kalshi WebSocket")
    }
}

#[async_trait]
impl ServiceFactory for KalshiService {
    async fn factory(services: ServiceProvider) -> AppResult<Self> {
        let config_service = services.get_service_unchecked::<ConfigService>().await;

        Ok(Self::new(config_service).await?)
    }
}
