// File: service/src/strategy/mod.rs

pub mod book;
pub mod events;
pub mod handler;
pub mod manager;
pub mod market;
pub mod oracle;
pub mod orderbook;
pub mod risk;
pub mod state;

use kalshi_rs::websocket::models::UserFillMessage;
use polymarket_client_sdk::clob::ws::TradeMessage;
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use rust_decimal::prelude::{FromPrimitive, ToPrimitive};

#[derive(Debug, Clone)]
pub struct Order {
    pub price_cents: u32,
    pub shares: u32,
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

/// Trait to unify access to position details
pub trait PositionProvider {
    fn get_id(&self) -> String;
    fn get_shares(&self) -> Decimal;
    fn get_side(&self) -> String;
    fn get_timestamp(&self) -> i64;
    fn get_price(&self) -> Decimal;
}

/// Specific implementation for Polymarket
#[derive(Debug, Clone)]
pub struct PolymarketPosition {
    pub order_id: String,
    pub token_id: U256,
    pub side: String, // "yes" or "no"
    pub shares: Decimal,
    pub price: Decimal,
    pub timestamp: i64,
}

impl PositionProvider for PolymarketPosition {
    fn get_id(&self) -> String {
        self.order_id.clone()
    }

    fn get_shares(&self) -> Decimal {
        self.shares
    }

    fn get_side(&self) -> String {
        self.side.clone()
    }

    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    fn get_price(&self) -> Decimal {
        self.price
    }
}

/// Specific implementation for Kalshi
#[derive(Debug, Clone)]
pub struct KalshiPosition {
    pub order_id: String,
    pub ticker: String,
    pub side: String, // "yes" or "no"
    pub shares: u32,
    pub price_cents: u32,
    pub timestamp: i64,
}

impl PositionProvider for KalshiPosition {
    fn get_id(&self) -> String {
        self.order_id.clone()
    }

    fn get_shares(&self) -> Decimal {
        Decimal::from(self.shares)
    }

    fn get_side(&self) -> String {
        self.side.clone()
    }

    fn get_timestamp(&self) -> i64 {
        self.timestamp
    }

    fn get_price(&self) -> Decimal {
        Decimal::from(self.price_cents) / Decimal::from(100)
    }
}

/// Tracks the state of an active arbitrage position (Holds explicit legs)
#[derive(Debug, Clone)]
pub struct PairedPosition {
    pub polymarket: PolymarketPosition,
    pub kalshi: KalshiPosition,
}

impl PairedPosition {
    pub fn new(polymarket: PolymarketPosition, kalshi: KalshiPosition) -> Self {
        Self { polymarket, kalshi }
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
