pub mod book;
pub mod market;

use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;

#[derive(Debug, Clone)] 
pub struct Order {
    pub price_cents: u32,
    pub contracts: u32
}

#[derive(Debug, Clone)]
pub struct ArbSizing {
    pub quantity: u32,
    pub total_cost_usd: Decimal,
    pub poly_cost_usd: Decimal,
    pub kalshi_cost_usd: Decimal,
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
    /// Calculates max contracts based on integer cent prices for both legs.
    /// 
    /// inputs:
    /// - budget_usd: Total limit in USD (e.g., 50.0)
    /// - poly_price_cents: Price in cents (e.g., 53)
    /// - kalshi_price_cents: Price in cents (e.g., 42)
    pub fn calculate(
        budget_usd: Decimal,
        poly_price_cents: u32,
        kalshi_price_cents: u32
    ) -> Option<Self> {
        // Convert Budget to Cents
        let budget_cents = (budget_usd * Decimal::from(100)).to_u32()?;
        
        let total_cost_per_unit_cents = poly_price_cents + kalshi_price_cents;

        // Avoid division by zero
        if total_cost_per_unit_cents == 0 {
            return None;
        }

        // Calculate quantity: Floor(TotalBudgetCents / CostPerUnitCents)
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