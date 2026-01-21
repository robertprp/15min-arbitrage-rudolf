use anyhow::bail;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{info, warn};
use polymarket_client_sdk::clob::types::Side;

// Updated imports
use crate::strategy::{FillEvent, PairedPosition};
use crate::market::Exchange; // Needed to match exchange strings to Enum if strict, but here we do simple string matching

pub struct PositionHandler {
    pub current_position: Option<PairedPosition>, 
    pub realized_pnl: Decimal,
}

impl PositionHandler {
    pub fn new() -> Self {
        Self {
            current_position: None,
            realized_pnl: Decimal::ZERO,
        }
    }

    pub fn set_position(&mut self, pos: PairedPosition) {
        self.current_position = Some(pos);
    }

    pub fn clear_position(&mut self) {
        self.current_position = None;
    }
    
    pub fn has_position(&self) -> bool {
        self.current_position.is_some()
    }

    /// Central entry point for any trade/fill event from any exchange
    pub fn handle_fill(&mut self, event: FillEvent) {
        info!("Handling fill event {:?}", event);
        match event {
            FillEvent::Poly(trade) => {
                let side_str = match trade.side {
                    Side::Buy => "Buy",
                    Side::Sell => "Sell",
                    _ => return
                };
                
                info!(
                    "🧩 [Poly Fill] Side: {}, Size: {}, Price: {}, Asset: {}", 
                    side_str, trade.size, trade.price, trade.asset_id
                );

                self.update_position_state("polymarket", side_str, trade.size, trade.price);
            }
            FillEvent::Kalshi(fill) => {
                let count_dec = Decimal::from(fill.count);
                let price_dec = Decimal::from(fill.yes_price) / Decimal::from(100);

                info!(
                    "🧩 [Kalshi Fill] Action: {}, Count: {}, Price: {}, Ticker: {}", 
                    fill.action, fill.count, price_dec, fill.market_ticker
                );

                self.update_position_state("kalshi", &fill.action, count_dec, price_dec);
            }
        }
    }

    /// Core logic to update the tracking struct
    fn update_position_state(&mut self, exchange_name: &str, side: &str, quantity: Decimal, price: Decimal) {
        let is_buy = side.eq_ignore_ascii_case("buy");

        if let Some(paired_pos) = &mut self.current_position {
            if !is_buy {
                // We are selling/unwinding
                // We need to find which leg of the pair corresponds to this exchange to calculate PnL
                
                // Find the specific position leg
                let matching_leg = paired_pos.positions.iter().find(|p| {
                     match p.exchange {
                         Exchange::Polymarket => exchange_name == "polymarket",
                         Exchange::Kalshi => exchange_name == "kalshi",
                     }
                });

                if let Some(leg) = matching_leg {
                    info!("📉 Reducing position on {} by {}", exchange_name, quantity);
                    
                    // Use leg.price (entry price) instead of entry_cost_per_unit
                    let profit = (price - leg.price) * quantity;
                    self.realized_pnl += profit;
                    
                    info!("💰 Realized PnL update: {}", self.realized_pnl);
                } else {
                    warn!("Received sell fill for {}, but no matching leg found in current position", exchange_name);
                }
            }
        } else if is_buy {
            // We are entering
            // The `monitor.rs` creates the initial Position struct on "Intent",
            // but the Handler verifies it on "Confirmation".
            info!("📈 Confirmed entry fill on {} for {}", exchange_name, quantity);
        }
    }
}