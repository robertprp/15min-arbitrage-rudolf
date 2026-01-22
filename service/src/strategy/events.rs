use kalshi_rs::websocket::models::{OrderbookDeltaMessage};
use polymarket_client_sdk::clob::ws::BestBidAsk;

use crate::market::exchange::Exchange;

#[derive(Debug)]
pub enum OrderbookChangeEvent {
    Polymarket(BestBidAsk),
    Kalshi(OrderbookDeltaMessage)
}
