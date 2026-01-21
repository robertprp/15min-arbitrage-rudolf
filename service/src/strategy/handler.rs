use anyhow::bail;
// service/src/strategy/handler.rs
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{info, warn};
use polymarket_client_sdk::clob::types::Side;

use crate::strategy::{FillEvent, Position};

/// Manages the state of the active position based on execution reports
pub struct PositionHandler {
    pub current_position: Option<Position>,
    pub realized_pnl: Decimal,
}

impl PositionHandler {
    pub fn new() -> Self {
        Self {
            current_position: None,
            realized_pnl: Decimal::ZERO,
        }
    }

    /// Central entry point for any trade/fill event from any exchange
    pub fn handle_fill(&mut self, event: FillEvent) {
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
    fn update_position_state(&mut self, exchange: &str, side: &str, quantity: Decimal, price: Decimal) {
        // logic:
        // 1. If we have NO position and side is BUY -> We are entering (partially or fully)
        // 2. If we HAVE position and side is SELL -> We are exiting
        // Note: This is a simplified state machine assuming the bot only goes Long -> Short.
        
        let is_buy = side.eq_ignore_ascii_case("buy");

        if let Some(pos) = &mut self.current_position {
            if !is_buy {
                // We are selling/unwinding
                info!("📉 Reducing position on {} by {}", exchange, quantity);
                // In a real scenario, you would decrement pos.quantity here.
                // For this MVP, if we sell, we assume we are closing.
                
                let profit = (price - pos.entry_cost_per_unit) * quantity;
                self.realized_pnl += profit;
                
                // If this looks like a full exit (heuristic), clear position
                // In a production bot, you'd track cumulative_filled_quantity
                info!("💰 Realized PnL update: {}", self.realized_pnl);
            }
        } else if is_buy {
            // We are entering
            // The `monitor.rs` creates the initial Position struct on "Intent",
            // but the Handler verifies it on "Confirmation".
            info!("📈 Confirmed entry fill on {} for {}", exchange, quantity);
        }
    }

    pub fn set_position(&mut self, pos: Position) {
        self.current_position = Some(pos);
    }

    pub fn clear_position(&mut self) {
        self.current_position = None;
    }
    
    pub fn has_position(&self) -> bool {
        self.current_position.is_some()
    }
}