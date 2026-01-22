use kalshi_rs::websocket::models::{OrderbookDeltaMessage, OrderbookSnapshotMessage};
use polymarket_client_sdk::clob::ws::BestBidAsk;
use rust_decimal::Decimal;

use crate::strategy::PairedPosition;

#[derive(Debug)]
pub enum OrderbookChangeEvent {
    Polymarket(BestBidAsk),
    KalshiDelta(OrderbookDeltaMessage),
    KalshiSnapshot(OrderbookSnapshotMessage),
}

#[derive(Debug)]
pub struct PriceChange {
    pub timestamp_secs: u64,
    pub price: Decimal,
}

#[derive(Debug)]
pub enum PriceChangeEvent {
    Polymarket(PriceChange),
    Kalshi(PriceChange),
}

pub enum PositionUpdateEvent {
    Created(PairedPosition),
}