use std::{str::FromStr, sync::Arc};

use anyhow::Context;
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

use crate::strategy::market::Opportunity; // Add this
use rust_decimal::prelude::ToPrimitive; // Needed for u32 conversion

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
        })
    }
    
    async fn handle_event(&self, event: OrderbookChangeEvent) {
            match event {
                OrderbookChangeEvent::Polymarket(bba) => self.handle_polymarket_update(bba).await,
                OrderbookChangeEvent::Kalshi(delta) => self.handle_kalshi_update(delta).await,
            }
    
            // Check for arbitrage immediately after update
            self.check_opportunities().await;
        }
    
        async fn check_opportunities(&self) {
            // 1. Acquire Read Locks
            // We lock all 4 simultaneously to ensure we are checking a consistent state 
            // (though in practice, updates happen sequentially per event)
            let pm_yes = self.pm_orderbook_yes.read().await;
            let pm_no = self.pm_orderbook_no.read().await;
            let k_yes = self.k_orderbook_yes.read().await;
            let k_no = self.k_orderbook_no.read().await;
    
            let one = Decimal::from(1);
            let one_hundred = Decimal::from(100);
    
            // --- Strategy A: Buy Poly YES + Buy Kalshi NO ---
            if let (Some(p_ask), Some(k_ask)) = (pm_yes.best_ask(), k_no.best_ask()) {
                let total_cost = p_ask + k_ask + self.min_edge;
                
                if total_cost < one {
                    let k_cents = (k_ask * one_hundred).ceil().to_u32().unwrap_or(99);
                    
                    let opp = Opportunity {
                        poly_token_id: self.poly_yes_asset,
                        poly_price: p_ask,
                        kalshi_ticker: self.kalshi_ticker.clone(),
                        kalshi_side: "no".to_string(),
                        kalshi_price_cents: k_cents,
                    };
    
                    info!("🚀 Arb Opportunity Found (A): {:?}", opp);
                    // TODO: Send to execution channel
                    // self.opportunity_tx.send(opp).await;
                }
            }
    
            // --- Strategy B: Buy Poly NO + Buy Kalshi YES ---
            if let (Some(p_ask), Some(k_ask)) = (pm_no.best_ask(), k_yes.best_ask()) {
                let total_cost = p_ask + k_ask + self.min_edge;
    
                if total_cost < one {
                    let k_cents = (k_ask * one_hundred).ceil().to_u32().unwrap_or(99);
    
                    let opp = Opportunity {
                        poly_token_id: self.poly_no_asset,
                        poly_price: p_ask,
                        kalshi_ticker: self.kalshi_ticker.clone(),
                        kalshi_side: "yes".to_string(),
                        kalshi_price_cents: k_cents,
                    };
    
                    info!("🚀 Arb Opportunity Found (B): {:?}", opp);
                    // TODO: Send to execution channel
                    // self.opportunity_tx.send(opp).await;
                }
            }
        }
        
    async fn handle_polymarket_update(&self, update: BestBidAsk) {
        // Determine which book to update based on asset_id
        let book = if update.asset_id == self.poly_yes_asset {
            &self.pm_orderbook_yes
        } else if update.asset_id == self.poly_no_asset {
            &self.pm_orderbook_no
        } else {
            // Received update for unknown asset
            return;
        };

        let mut book_write = book.write().await;

        // Polymarket BBA is a snapshot of the top of the book.
        // We clear existing levels to reflect strictly the best price available.
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

        // "yes" side updates the YES book directly, and the NO book synthetically (inverted)
        // "no" side updates the NO book directly, and the YES book synthetically (inverted)
        match msg.side.as_str() {
            "yes" => {
                // 1. Direct: Kalshi Yes Bid
                Self::apply_delta_to_book(&self.k_orderbook_yes, true, price, delta_qty).await;

                // 2. Synthetic: Kalshi No Ask (1 - Yes Bid)
                if price < one {
                    let inverted_price = one - price;
                    Self::apply_delta_to_book(&self.k_orderbook_no, false, inverted_price, delta_qty).await;
                }
            }
            "no" => {
                // 1. Direct: Kalshi No Bid
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