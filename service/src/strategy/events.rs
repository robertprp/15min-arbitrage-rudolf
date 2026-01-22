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

#[derive(Clone, Debug)]
pub struct PriceChange {
    pub timestamp_secs: u64,
    pub price: Decimal,
}

#[derive(Clone, Debug)]
pub enum PriceChangeEvent {
    Polymarket(PriceChange),
    Kalshi(PriceChange),
}

#[derive(Clone, Debug)]
pub enum PositionUpdateEvent {
    Created(PairedPosition),
}

#[derive(Clone, Debug)]
pub enum OracleUpdateEvent {
    Polymarket { price: Decimal, timestamp: i64 },
    Kalshi { price: Decimal, timestamp: i64 },
}
