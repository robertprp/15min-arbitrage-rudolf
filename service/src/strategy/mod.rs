pub mod book;
pub mod handler;
pub mod market;

use kalshi_rs::websocket::models::UserFillMessage;
use polymarket_client_sdk::clob::ws::TradeMessage;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use polymarket_client_sdk::types::U256;

#[derive(Debug, Clone)]
pub struct Order {
    pub price_cents: u32,
    pub contracts: u32
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
    pub quantity: u32,
    pub entry_cost_per_unit: Decimal,
    pub entry_time: std::time::Instant,

    // Asset Identifiers
    pub poly_token_id: U256,
    pub poly_side: String, // "buy" (entry) -> "sell" (exit) implies we hold the asset
    pub kalshi_ticker: String,
    pub kalshi_side: String, // "yes" or "no"
}

impl Order {
    pub fn new(price_cents: u32, contracts: u32) -> Self {
        Order {
            price_cents,
            contracts
        }
    }

    pub fn price_usd(&self) -> f64 {
        (self.price_cents as f64 / 100.0) * self.contracts as f64
    }
}

impl ArbSizing {
    pub fn calculate(
        budget_usd: Decimal,
        poly_price_cents: u32,
        kalshi_price_cents: u32
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
