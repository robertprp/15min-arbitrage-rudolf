use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use polymarket_client_sdk::clob::types::{OrderType, Side};
use polymarket_client_sdk::clob::ws::BestBidAsk;
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use rust_decimal::prelude::{ToPrimitive, FromPrimitive};
use tokio::sync::mpsc;
use tracing::{info, warn, error};

use crate::polymarket::clob::{PolymarketClobService, PolymarketExecutionService};
use crate::polymarket::ws::PolymarketFeedUpdate;
use crate::config::service::ConfigService;

#[derive(Debug, Clone)]
pub struct MarketMakerConfig {
    pub asset_id: U256,
    pub spread: Decimal,           // e.g., 0.02 (2%)
    pub order_size_usdc: Decimal,  // e.g., 10 USDC
    pub max_inventory: Decimal,    // Max shares to hold before skewing hard
    pub max_inventory_usd: Decimal, // Max USD value in positions
    pub skew_factor: Decimal,      // How much to move price per share of inventory
    pub min_price_change: Decimal, // Only update if price moves > this (e.g., 0.005)
}

pub struct MarketMaker {
    config: MarketMakerConfig,
    clob_service: Arc<PolymarketClobService>,
    exec_service: Arc<PolymarketExecutionService>,
    
    // State
    pub inventory: Decimal,         // Shares held (for skew calculation)
    pub inventory_usd: Decimal,     // USD value of positions
    pub active_orders: Vec<String>, // Order IDs
    pub last_mid_price: Decimal,
    pub last_update: Instant,
}

impl MarketMaker {
    pub async fn new(
        config_service: Arc<ConfigService>,
        clob_service: Arc<PolymarketClobService>,
        asset_id: U256,
        max_inventory_usd: Decimal,
    ) -> Result<Self> {
        let exec_service = Arc::new(
            PolymarketExecutionService::new(
                clob_service.clone(),
                config_service.clone(),
                asset_id,
            ).await?
        );

        Ok(Self {
            config: MarketMakerConfig {
                asset_id,
                spread: Decimal::from_f64(0.02).unwrap(), // 2% Spread
                order_size_usdc: Decimal::from(10),       // 10 USDC per side
                max_inventory: Decimal::from(1000),
                max_inventory_usd,                        // Max USD in positions
                skew_factor: Decimal::from_f64(0.0001).unwrap(), // 0.01 cents per share
                min_price_change: Decimal::from_f64(0.005).unwrap(),
            },
            clob_service,
            exec_service,
            inventory: Decimal::ZERO,     // Shares held
            inventory_usd: Decimal::ZERO, // USD value of positions
            active_orders: Vec::new(),
            last_mid_price: Decimal::ZERO,
            last_update: Instant::now(),
        })
    }

    /// Update Inventory (Call this when a trade fills)
    /// - `share_change`: positive for buys, negative for sells
    /// - `price`: the fill price to calculate USD value
    pub fn update_inventory(&mut self, share_change: Decimal, price: Decimal) {
        self.inventory += share_change;
        let usd_change = share_change * price;
        self.inventory_usd += usd_change;
        info!("📊 Inventory Updated: {} shares, {} USD", self.inventory, self.inventory_usd);
    }

    /// Calculate remaining USD capacity for new positions
    fn remaining_usd_capacity(&self) -> Decimal {
        (self.config.max_inventory_usd - self.inventory_usd).max(Decimal::ZERO)
    }

    /// Main logic to recalculate and replace orders
    pub async fn on_market_data(&mut self, bba: BestBidAsk) -> Result<()> {
        let best_bid = bba.best_bid;
        let best_ask = bba.best_ask;

        if best_bid.is_zero() || best_ask.is_zero() {
            return Ok(());
        }

        let mid_price = (best_bid + best_ask) / Decimal::from(2);

        // 1. Check Threshold: Don't spam updates if price hasn't moved much
        let change = (mid_price - self.last_mid_price).abs();
        if change < self.config.min_price_change && !self.active_orders.is_empty() {
            return Ok(());
        }

        self.last_mid_price = mid_price;

        // 2. Calculate Skewed Mid Price (Avellaneda-Stoikov logic)
        // If we have positive inventory, we lower price to sell.
        // If we have negative (short) inventory, we raise price to buy.
        let reservation_price = mid_price - (self.inventory * self.config.skew_factor);

        // 3. Calculate Quotes
        let half_spread = self.config.spread / Decimal::from(2);
        let target_bid = reservation_price - half_spread;
        let target_ask = reservation_price + half_spread;

        // Safety clamps
        let target_bid = target_bid.min(Decimal::from_f64(0.99).unwrap()).max(Decimal::from_f64(0.01).unwrap());
        let target_ask = target_ask.min(Decimal::from_f64(0.99).unwrap()).max(Decimal::from_f64(0.01).unwrap());

        info!(
            "🎯 Remaking Market: Mid: {:.3} -> Skewed: {:.3} | Bid: {:.3} | Ask: {:.3} | Inv: {}", 
            mid_price, reservation_price, target_bid, target_ask, self.inventory
        );

        // 4. Cancel & Replace
        self.refresh_orders(target_bid, target_ask).await?;

        Ok(())
    }

    async fn refresh_orders(&mut self, bid_price: Decimal, ask_price: Decimal) -> Result<()> {
        // A. Cancel Existing
        if !self.active_orders.is_empty() {
            // We ignore errors on cancel to ensure we proceed to placing new orders
            if let Err(e) = self.clob_service.cancel_orders(self.active_orders.clone()).await {
                warn!("Failed to cancel previous orders: {:?}", e);
            }
            self.active_orders.clear();
        }

        // B. Calculate order sizes based on remaining USD capacity
        let remaining_capacity = self.remaining_usd_capacity();
        
        // For BUYS: we're adding to our position (increasing USD exposure)
        // Calculate how much we can afford to buy
        let bid_usdc = self.config.order_size_usdc.min(remaining_capacity);
        let bid_size_shares = if bid_usdc > Decimal::ZERO {
            bid_usdc / bid_price
        } else {
            Decimal::ZERO
        };

        // For SELLS: we're reducing our position, so no inventory limit applies
        // (selling reduces USD exposure)
        let ask_size_shares = self.config.order_size_usdc / ask_price;

        // C. Build Orders (Post Only to ensure Maker Rebates)

        // Only place bid if we have capacity
        if bid_size_shares > Decimal::ZERO {
            match self.exec_service.build_limit_order(bid_price, bid_size_shares, Side::Buy) {
                Ok(mut bid_order) => {
                    bid_order.post_only = Some(true);
                    match self.exec_service.execute_order(bid_order).await {
                        Ok(res) => {
                            self.active_orders.push(res.order_id);
                        }
                        Err(e) => warn!("Failed to place Bid: {:?}", e),
                    }
                }
                Err(e) => warn!("Failed to build Bid order: {:?}", e),
            }
        } else {
            info!("📊 Skipping bid: inventory limit reached (USD: {}/{})", 
                  self.inventory_usd, self.config.max_inventory_usd);
        }

        // Always place ask (sells reduce inventory)
        match self.exec_service.build_limit_order(ask_price, ask_size_shares, Side::Sell) {
            Ok(mut ask_order) => {
                ask_order.post_only = Some(true);
                match self.exec_service.execute_order(ask_order).await {
                    Ok(res) => {
                        self.active_orders.push(res.order_id);
                    }
                    Err(e) => warn!("Failed to place Ask: {:?}", e),
                }
            }
            Err(e) => warn!("Failed to build Ask order: {:?}", e),
        }

        self.last_update = Instant::now();
        Ok(())
    }
}