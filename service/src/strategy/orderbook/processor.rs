use std::{str::FromStr, sync::Arc};

use anyhow::Context;
use kalshi_rs::websocket::models::OrderbookSnapshotMessage;
use lib::AppResult;
use polymarket_client_sdk::clob::ws::BestBidAsk;
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use tokio::{
    select,
    sync::{broadcast, mpsc, RwLock},
    task::JoinHandle,
};
use tracing::{error, info, warn};

use crate::{
    services::{ServiceFactory, ServiceProvider},
    strategy::{
        book::Orderbook,
        events::OrderbookChangeEvent,
    },
};

use crate::strategy::market::Opportunity; 
use rust_decimal::prelude::ToPrimitive; 

#[derive(Clone)]
pub struct OrderbookProcessor {
    pub services: ServiceProvider,

    // Market Identifiers for Routing
    poly_yes_asset: U256,
    poly_no_asset: U256,
    kalshi_ticker: String,

    min_edge: Decimal,

    // State
    pub pm_orderbook_yes: Arc<RwLock<Orderbook>>,
    pub pm_orderbook_no: Arc<RwLock<Orderbook>>,

    pub k_orderbook_yes: Arc<RwLock<Orderbook>>,
    pub k_orderbook_no: Arc<RwLock<Orderbook>>,

    // Channels
    event_rx: Arc<RwLock<Option<mpsc::Receiver<OrderbookChangeEvent>>>>,
    event_tx: mpsc::Sender<OrderbookChangeEvent>,

    shutdown_rx: Arc<RwLock<Option<broadcast::Receiver<()>>>>,
    shutdown_tx: broadcast::Sender<()>,
}

impl OrderbookProcessor {
    pub fn new(
        services: ServiceProvider,
        poly_yes_asset: U256,
        poly_no_asset: U256,
        kalshi_ticker: String,
        min_edge: Decimal
    ) -> Self {
        let (event_tx, event_rx) = mpsc::channel(20_000);
        let (shutdown_tx, shutdown_rx) = broadcast::channel(1);

        Self {
            services,
            poly_yes_asset,
            poly_no_asset,
            kalshi_ticker,
            min_edge,
            pm_orderbook_yes: Arc::new(RwLock::new(Orderbook::new())),
            
            pm_orderbook_no: Arc::new(RwLock::new(Orderbook::new())),
            k_orderbook_yes: Arc::new(RwLock::new(Orderbook::new())),
            k_orderbook_no: Arc::new(RwLock::new(Orderbook::new())),
            event_rx: Arc::new(RwLock::new(Some(event_rx))),
            event_tx,
            shutdown_rx: Arc::new(RwLock::new(Some(shutdown_rx))),
            shutdown_tx,
        }
    }

    /// Returns a sender to push events into this processor
    pub fn get_event_sender(&self) -> mpsc::Sender<OrderbookChangeEvent> {
        self.event_tx.clone()
    }

    pub async fn start(&self) -> JoinHandle<AppResult<()>> {
        let mut event_rx = self.event_rx.write().await.take().expect("Event RX already taken");
        let mut shutdown_rx = self.shutdown_rx.write().await.take().expect("Shutdown RX already taken");
        
        let processor = self.clone();

        tokio::spawn(async move {
            info!("📘 OrderbookProcessor started");

            loop {
                select! {
                    event = event_rx.recv() => {
                        match event {
                            Some(change) => processor.handle_event(change).await,
                            None => {
                                warn!("Orderbook event channel closed");
                                break;
                            }
                        }
                    }
                    _ = shutdown_rx.recv() => {
                        info!("🛑 OrderbookProcessor received shutdown signal");
                        break;
                    }
                }
            }

            info!("📘 OrderbookProcessor stopped");
            Ok(())
        }
        )
    }
    
    async fn handle_event(&self, event: OrderbookChangeEvent) {
        match event {
            OrderbookChangeEvent::Polymarket(bba) => self.handle_polymarket_update(bba).await,
            OrderbookChangeEvent::KalshiDelta(delta) => self.handle_kalshi_update(delta).await,
            OrderbookChangeEvent::KalshiSnapshot(snapshot) => self.handle_kalshi_snapshot(snapshot).await,
        }
        
        // Check for arbitrage immediately after update
        self.check_opportunities().await;
    }

    async fn handle_kalshi_snapshot(&self, msg: OrderbookSnapshotMessage) {
        if msg.market_ticker != self.kalshi_ticker {
            return;
        }
        
        let mut k_yes = self.k_orderbook_yes.write().await;
        let mut k_no = self.k_orderbook_no.write().await;
        
        // 1. Reset Books (Snapshot is the source of truth)
        k_yes.bids.clear();
        k_yes.asks.clear();
        k_no.bids.clear();
        k_no.asks.clear();
        
        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);
        
        // "yes" in the snapshot = Bids on the Yes side
        let yes_bids = msg.yes.unwrap_or_default();
        // "no" in the snapshot = Bids on the No side
        let no_bids = msg.no.unwrap_or_default();

        // 2. Process YES Bids
        for (price_cents, qty) in yes_bids {
            let price = Decimal::from(price_cents) / one_hundred;
            let quantity = Decimal::from(qty);
            
            // Direct: YES Bid
            k_yes.update_bid(price, quantity);
            
            // Synthetic: NO Ask (1 - Price)
            // Buying Yes at 0.40 is equivalent to Selling No at 0.60
            if price < one {
                let inverted_price = one - price;
                k_no.update_ask(inverted_price, quantity);
            }
        }
        
        // 3. Process NO Bids
        for (price_cents, qty) in no_bids {
            let price = Decimal::from(price_cents) / one_hundred;
            let quantity = Decimal::from(qty);
            
            // Direct: NO Bid
            k_no.update_bid(price, quantity);
            
            // Synthetic: YES Ask (1 - Price)
            // Buying No at 0.40 is equivalent to Selling Yes at 0.60
            if price < one {
                let inverted_price = one - price;
                k_yes.update_ask(inverted_price, quantity);
            }
        }
        
        info!("✅ Kalshi Snapshot Processed.");
        // Debug logs to verify the fix
        if let Some(best_ask) = k_yes.best_ask() {
             info!("Updated Kalshi YES Best Ask: {}", best_ask);
        }
    }
    
    async fn check_opportunities(&self) {
        // 1. Acquire Read Locks
        let pm_yes = self.pm_orderbook_yes.read().await;
        let pm_no = self.pm_orderbook_no.read().await;
        let k_yes = self.k_orderbook_yes.read().await;
        let k_no = self.k_orderbook_no.read().await;
        
        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);
        
        // --- Strategy A: Buy Poly YES + Buy Kalshi NO ---
        // (Buying Kalshi NO is equivalent to selling Kalshi YES)
        // We compare Poly Ask (Cost to buy Yes) vs Kalshi No Ask (Cost to Buy No, which is Synth Sell Yes)
        // Wait: To Arb, we want to Buy Low on one, Sell High on other.
        // Buy Poly YES (at Ask) + Sell Kalshi YES (by Buying Kalshi NO at Ask? No.)
        
        // Correct Arb Logic:
        // We want to lock in a profit > 0.
        // Cost 1: Buy Poly Yes
        // Cost 2: Buy Kalshi No (Equivalent to covering the other side)
        // If (Poly Yes Price + Kalshi No Price) < $1.00, we have risk-free profit.
        
        if let (Some(p_ask), Some(k_ask_no)) = (pm_yes.best_ask(), k_no.best_ask()) {
            let total_cost = p_ask + k_ask_no;
            
            if total_cost + self.min_edge < one {
                let k_cents = (k_ask_no * one_hundred).ceil().to_u32().unwrap_or(99);
                
                let opp = Opportunity {
                    poly_token_id: self.poly_yes_asset,
                    poly_price: p_ask,
                    kalshi_ticker: self.kalshi_ticker.clone(),
                    kalshi_side: "no".to_string(),
                    kalshi_price_cents: k_cents,
                };
                
                info!("🚀 Arb Opportunity Found (A): Cost {} (P: {} + K_No: {})", total_cost, p_ask, k_ask_no);
                // self.event_tx.... (connect to arb processor)
            }
        }
        
        // --- Strategy B: Buy Poly NO + Buy Kalshi YES ---
        if let (Some(p_ask), Some(k_ask_yes)) = (pm_no.best_ask(), k_yes.best_ask()) {
            let total_cost = p_ask + k_ask_yes;
            
            if total_cost + self.min_edge < one {
                let k_cents = (k_ask_yes * one_hundred).ceil().to_u32().unwrap_or(99);
                
                let opp = Opportunity {
                    poly_token_id: self.poly_no_asset,
                    poly_price: p_ask,
                    kalshi_ticker: self.kalshi_ticker.clone(),
                    kalshi_side: "yes".to_string(),
                    kalshi_price_cents: k_cents,
                };
                
                info!("🚀 Arb Opportunity Found (B): Cost {} (P: {} + K_Yes: {})", total_cost, p_ask, k_ask_yes);
            }
        }
    }
    
    async fn handle_polymarket_update(&self, update: BestBidAsk) {
        let book = if update.asset_id == self.poly_yes_asset {
            &self.pm_orderbook_yes
        } else if update.asset_id == self.poly_no_asset {
            &self.pm_orderbook_no
        } else {
            return;
        };

        let mut book_write = book.write().await;

        book_write.bids.clear();
        book_write.asks.clear();

        if !update.best_bid.is_zero() {
            book_write.update_bid(update.best_bid, Decimal::from(1));
        }
        if !update.best_ask.is_zero() {
            book_write.update_ask(update.best_ask, Decimal::from(1));
        }
    }

    async fn handle_kalshi_update(&self, msg: kalshi_rs::websocket::models::OrderbookDeltaMessage) {
        if msg.market_ticker != self.kalshi_ticker {
            return;
        }

        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);

        let price_cents = msg.price;
        let delta_qty = Decimal::from(msg.delta); 
        let price = Decimal::from(price_cents) / one_hundred;

        // Note: Delta messages from Kalshi are usually updates to Bids (on either Yes or No side).
        match msg.side.as_str() {
            "yes" => {
                // 1. Direct: Kalshi Yes Bid Updated
                Self::apply_delta_to_book(&self.k_orderbook_yes, true, price, delta_qty).await;

                // 2. Synthetic: Kalshi No Ask (1 - Yes Bid)
                if price < one {
                    let inverted_price = one - price;
                    Self::apply_delta_to_book(&self.k_orderbook_no, false, inverted_price, delta_qty).await;
                }
            }
            "no" => {
                // 1. Direct: Kalshi No Bid Updated
                Self::apply_delta_to_book(&self.k_orderbook_no, true, price, delta_qty).await;

                // 2. Synthetic: Kalshi Yes Ask (1 - No Bid)
                if price < one {
                    let inverted_price = one - price;
                    Self::apply_delta_to_book(&self.k_orderbook_yes, false, inverted_price, delta_qty).await;
                }
            }
            _ => warn!("Unknown Kalshi side: {}", msg.side),
        }
    }

    /// Helper to safely apply delta to a locked orderbook
    async fn apply_delta_to_book(
        book: &Arc<RwLock<Orderbook>>, 
        is_bid: bool, 
        price: Decimal, 
        delta: Decimal
    ) {
        let mut book_write = book.write().await;
        
        let current_qty = if is_bid {
            book_write.bids.get(&price).cloned().unwrap_or(Decimal::ZERO)
        } else {
            book_write.asks.get(&price).cloned().unwrap_or(Decimal::ZERO)
        };

        let new_qty = current_qty + delta;

        if is_bid {
            book_write.update_bid(price, new_qty);
        } else {
            book_write.update_ask(price, new_qty);
        }
    }
}