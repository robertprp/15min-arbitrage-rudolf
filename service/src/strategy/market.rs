use crate::market::metadata::MarketMetadata;
use crate::strategy::book::Orderbook;
use kalshi_rs::markets::models::GetMarketOrderbookResponse;
use kalshi_rs::websocket::models::{
    OrderbookDelta, OrderbookSnapshot, OrderbookSnapshotMessage, TickerUpdateMessage,
};
use polymarket_client_sdk::{clob::ws::BestBidAsk, types::U256};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::str::FromStr;
use tracing::{info, warn};
// Import PairedPosition and Exchange
use crate::strategy::{PairedPosition, Position};

#[derive(Debug)]
pub struct Opportunity {
    // Execution details
    pub poly_token_id: U256,
    pub poly_price: Decimal,
    pub kalshi_ticker: String,
    pub kalshi_side: String, // "yes" or "no"
    pub kalshi_price_cents: u32,

    pub timestamp: u64,
}
