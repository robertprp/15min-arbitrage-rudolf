use kalshi_rs::websocket::models::{OrderbookDeltaMessage, OrderbookSnapshotMessage};
use polymarket_client_sdk::clob::ws::BestBidAsk;

#[derive(Debug)]
pub enum OrderbookChangeEvent {
    Polymarket(BestBidAsk),
    KalshiDelta(OrderbookDeltaMessage),
    KalshiSnapshot(OrderbookSnapshotMessage)
}