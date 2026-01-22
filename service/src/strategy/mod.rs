pub mod arbitrage;
pub mod book;
pub mod events;
pub mod handler;
pub mod manager;
pub mod market;
pub mod oracle;
pub mod orderbook;
pub mod risk;

use kalshi_rs::websocket::models::UserFillMessage;
use polymarket_client_sdk::clob::ws::TradeMessage;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

use crate::market::exchange::Exchange;

#[derive(Debug, Clone)]
pub struct Order {
    pub price_cents: u32, // 47c
    pub shares: u32,      // 1 shares.
                          // total cost 0.47$
}

#[derive(Debug)]
pub enum FillEvent {
    Poly(TradeMessage),
    Kalshi(UserFillMessage),
}

#[derive(Debug, Clone)]
pub struct ArbSizing {
    pub quantity: u32,
    pub total_cost_usd: Decimal,
    pub poly_cost_usd: Decimal,
    pub kalshi_cost_usd: Decimal,
}

/// Tracks the state of an active arbitrage position
#[derive(Debug, Clone)]
pub struct Position {
    pub exchange: Exchange, // "polymarket" or "kalshi"
    pub order_id: String,   // Token ID (Poly) or Ticker (Kalshi)
    pub side: String,       // "yes"/"no"
    pub timestamp: i64,
}

#[derive(Debug, Clone)]
pub struct PairedPosition {
    pub positions: [Position; 2],
}

impl PairedPosition {
    pub fn new(poly: Position, kalshi: Position) -> Self {
        Self {
            positions: [poly, kalshi],
        }
    }

    pub fn get_poly(&self) -> Option<&Position> {
        self.positions
            .iter()
            .find(|p| p.exchange == Exchange::Polymarket)
    }

    pub fn get_kalshi(&self) -> Option<&Position> {
        self.positions
            .iter()
            .find(|p| p.exchange == Exchange::Kalshi)
    }
}

impl Order {
    pub fn new(price_cents: u32, shares: u32) -> Self {
        Order {
            price_cents,
            shares,
        }
    }

    pub fn price_usd(&self) -> Decimal {
        let value = (self.price_cents as f64 / 100.0) * self.shares as f64;

        Decimal::from_f64(value).unwrap()
    }
}

impl ArbSizing {
    pub fn calculate(
        budget_usd: Decimal,
        poly_price_cents: u32,
        kalshi_price_cents: u32,
    ) -> Option<Self> {
        let budget_cents = (budget_usd * Decimal::from(100)).to_u32()?;

        let total_cost_per_unit_cents = poly_price_cents + kalshi_price_cents;

        if total_cost_per_unit_cents == 0 {
            return None;
        }

        let quantity = budget_cents / total_cost_per_unit_cents;

        if quantity == 0 {
            return None;
        }

        let quantity_dec = Decimal::from(quantity);
        let poly_cost = (Decimal::from(poly_price_cents) / Decimal::from(100)) * quantity_dec;
        let kalshi_cost = (Decimal::from(kalshi_price_cents) / Decimal::from(100)) * quantity_dec;

        Some(ArbSizing {
            quantity,
            total_cost_usd: poly_cost + kalshi_cost,
            poly_cost_usd: poly_cost,
            kalshi_cost_usd: kalshi_cost,
        })
    }
}
