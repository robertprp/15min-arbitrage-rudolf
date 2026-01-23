// File: service/src/strategy/orderbook/processor.rs


use futures::StreamExt; // Ensure futures is imported
use kalshi_rs::websocket::models::{OrderbookDeltaMessage, OrderbookSnapshotMessage};
use lib::AppResult;
use polymarket_client_sdk::clob::ws::BestBidAsk;
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use tokio::{
    select,
    sync::{broadcast, mpsc},
    task::JoinHandle, // Import interval
};
use tracing::{error, info, warn};

use crate::strategy::events::OracleUpdateEvent;
use crate::strategy::risk::layer::ActionEnter;
use crate::{
    chainlink::service::ChainLinkService, // Ensure this enum is available
    kalshi::service::KalshiService,
    market::metadata::MarketMetadata,
    polymarket::gamma::PolymarketGammaService,
    services::ServiceProvider,
    strategy::{
        book::Orderbook,
        events::OrderbookChangeEvent,
        market::Opportunity,
        risk::layer::RiskLayer, // Import the new RiskLayer
    },
};
use rust_decimal::prelude::ToPrimitive;

pub struct OrderbookProcessor {
    pub services: ServiceProvider,

    // Market Identifiers for Routing
    poly_yes_asset: U256,
    poly_no_asset: U256,
    kalshi_ticker: String,

    min_edge: Decimal,

    // -- Risk Management --
    enter_risk_layer: RiskLayer<ActionEnter>,

    pub pm_orderbook_yes: Orderbook,
    pub pm_orderbook_no: Orderbook,

    pub k_orderbook_yes: Orderbook,
    pub k_orderbook_no: Orderbook,

    pub position_count: u32,
    // Channels
    event_rx: mpsc::Receiver<OrderbookChangeEvent>,
    event_tx: mpsc::Sender<OrderbookChangeEvent>,

    executor_tx: mpsc::Sender<Opportunity>,
    oracle_rx: broadcast::Receiver<OracleUpdateEvent>, // Add this
}

impl OrderbookProcessor {
    pub async fn new(
        services: ServiceProvider,
        poly_yes_asset: U256,
        poly_no_asset: U256,
        poly_ticker: String,
        kalshi_event_ticker: String,
        kalshi_market_ticker: String,
        min_edge: Decimal,
        executor_tx: mpsc::Sender<Opportunity>,
        oracle_rx: broadcast::Receiver<OracleUpdateEvent>,
    ) -> AppResult<Self> {
        let (event_tx, event_rx) = mpsc::channel(20_000);
        let pm_gamma = services
            .get_service_unchecked::<PolymarketGammaService>()
            .await;
        let kalshi_service = services.get_service_unchecked::<KalshiService>().await;
        let chainlink_service = services.get_service_unchecked::<ChainLinkService>().await;

        let k_market = kalshi_service.get_event(&kalshi_event_ticker).await?;
        let pm_market = pm_gamma.get_market_by_slug(&poly_ticker).await?;

        let (_timestamp, strike_price) = chainlink_service.get_last_15min_open().await?;

        let k_metadata = MarketMetadata::from_kalshi(k_market)?;
        let pm_metadata = MarketMetadata::from_polymarket(pm_market, strike_price)?;

        // Initialize Risk Layers
        let enter_risk_layer = RiskLayer::new(pm_metadata.clone(), k_metadata.clone());

        let processor = Self {
            services,
            poly_yes_asset,
            poly_no_asset,
            kalshi_ticker: kalshi_market_ticker,
            enter_risk_layer,
            min_edge,
            executor_tx,
            position_count: 0,
            pm_orderbook_yes: Orderbook::new(),
            pm_orderbook_no: Orderbook::new(),
            k_orderbook_yes: Orderbook::new(),
            k_orderbook_no: Orderbook::new(),
            event_rx,
            event_tx,
            oracle_rx,
        };

        Ok(processor)
    }

    /// Returns a sender to push events into this processor
    pub fn get_event_sender(&self) -> mpsc::Sender<OrderbookChangeEvent> {
        self.event_tx.clone()
    }

    /// Consumes the processor and spawns the event loop
    pub async fn start(&mut self) -> AppResult<()> {
        info!("📘 OrderbookProcessor started");

        loop {
            select! {
                // --- Internal Orderbook Events ---
                event = self.event_rx.recv() => {
                    match event {
                        Some(change) => self.handle_event(change).await,
                        None => {
                            warn!("Orderbook event channel closed");
                            break;
                        }
                    }
                    }

                    Ok(oracle_msg) = self.oracle_rx.recv() => {
                        match oracle_msg {
                            OracleUpdateEvent::Polymarket { price, .. } => {
                                self.enter_risk_layer.update_poly_price(price);
                            }
                            OracleUpdateEvent::Kalshi { price, .. } => {
                                self.enter_risk_layer.update_kalshi_price(price);
                            }
                        }
                    }
            }
        }

        info!("📘 OrderbookProcessor stopped");
        Ok(())
    }

    async fn handle_event(&mut self, event: OrderbookChangeEvent) {
        match event {
            OrderbookChangeEvent::Polymarket(bba) => self.handle_polymarket_update(bba).await,
            OrderbookChangeEvent::KalshiDelta(delta) => self.handle_kalshi_update(delta).await,
            OrderbookChangeEvent::KalshiSnapshot(snapshot) => self.handle_kalshi_snapshot(snapshot).await
        }

        // Check for arbitrage immediately after update
        self.check_opportunities().await;
    }

    async fn check_opportunities(&mut self) {
        let pm_yes = &self.pm_orderbook_yes;
        let pm_no = &self.pm_orderbook_no;
        let k_yes = &self.k_orderbook_yes;
        let k_no = &self.k_orderbook_no;

        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);

        // --- Strategy A: Buy Poly YES + Buy Kalshi NO ---
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
                    timestamp: 0, //TODO
                };

                if self
                    .enter_risk_layer
                    .is_safe_to_enter(&opp, self.position_count)
                {
                    info!(
                        "🚀 Arb Opportunity Found (A): Cost {} (P: {} + K_No: {})",
                        total_cost, p_ask, k_ask_no
                    );
                    
                    if let Err(err) = self.executor_tx.send(opp).await {
                        error!("Failed to send opportunity: {}", err);
                    } else {
                        self.position_count += 1;
                    }
                    
                    return;
                }
            }
        }

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
                    timestamp: 0,
                };

                // RISKY CHECK
                if self
                    .enter_risk_layer
                    .is_safe_to_enter(&opp, self.position_count)
                {
                    info!(
                        "🚀 Arb Opportunity Found (B): Cost {} (P: {} + K_Yes: {})",
                        total_cost, p_ask, k_ask_yes
                    );

                    if let Err(err) = self.executor_tx.send(opp).await {
                        error!("Failed to send opportunity: {}", err);
                    } else {
                        self.position_count += 1;
                    }
                }
            }
        }
    }

    async fn handle_kalshi_snapshot(&mut self, msg: OrderbookSnapshotMessage) {
        if msg.market_ticker != self.kalshi_ticker {
            return;
        }

        let k_yes = &mut self.k_orderbook_yes;
        let k_no = &mut self.k_orderbook_no;

        // 1. Reset Books (Snapshot is the source of truth)
        k_yes.bids.clear();
        k_yes.asks.clear();
        k_no.bids.clear();
        k_no.asks.clear();

        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);

        let yes_bids = msg.yes.unwrap_or_default();
        let no_bids = msg.no.unwrap_or_default();

        for (price_cents, qty) in yes_bids {
            let price = Decimal::from(price_cents) / one_hundred;
            let quantity = Decimal::from(qty);

            k_yes.update_bid(price, quantity);

            if price < one {
                let inverted_price = one - price;
                k_no.update_ask(inverted_price, quantity);
            }
        }

        for (price_cents, qty) in no_bids {
            let price = Decimal::from(price_cents) / one_hundred;
            let quantity = Decimal::from(qty);

            k_no.update_bid(price, quantity);

            if price < one {
                let inverted_price = one - price;
                k_yes.update_ask(inverted_price, quantity);
            }
        }

        info!("✅ Kalshi Snapshot Processed.");
    }

    async fn handle_polymarket_update(&mut self, update: BestBidAsk) {
        let book = if update.asset_id == self.poly_yes_asset {
            &mut self.pm_orderbook_yes
        } else if update.asset_id == self.poly_no_asset {
            &mut self.pm_orderbook_no
        } else {
            return;
        };

        book.bids.clear();
        book.asks.clear();

        if !update.best_bid.is_zero() {
            book.update_bid(update.best_bid, Decimal::from(1));
        }

        if !update.best_ask.is_zero() {
            book.update_ask(update.best_ask, Decimal::from(1));
        }
    }

    async fn handle_kalshi_update(&mut self, msg: OrderbookDeltaMessage) {
        if msg.market_ticker != self.kalshi_ticker {
            return;
        }

        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);
        let price_cents = msg.price;
        let delta_qty = Decimal::from(msg.delta);
        let price = Decimal::from(price_cents) / one_hundred;

        match msg.side.as_str() {
            "yes" => {
                // 1. Direct: Kalshi Yes Bid Updated

                Self::apply_delta_to_book(&mut self.k_orderbook_yes, true, price, delta_qty);

                // 2. Synthetic: Kalshi No Ask (1 - Yes Bid)

                if price < one {
                    let inverted_price = one - price;

                    Self::apply_delta_to_book(
                        &mut self.k_orderbook_no,
                        false,
                        inverted_price,
                        delta_qty,
                    );
                }
            }

            "no" => {
                Self::apply_delta_to_book(&mut self.k_orderbook_no, true, price, delta_qty);

                if price < one {
                    let inverted_price = one - price;

                    Self::apply_delta_to_book(
                        &mut self.k_orderbook_yes,
                        false,
                        inverted_price,
                        delta_qty,
                    );
                }
            }

            _ => warn!("Unknown Kalshi side: {}", msg.side),
        }
    }

    fn apply_delta_to_book(
        book: &mut Orderbook, 
        is_bid: bool,
        price: Decimal,
        delta: Decimal,
    ) {
        let current_qty = if is_bid {
            book.bids.get(&price).cloned().unwrap_or(Decimal::ZERO)
        } else {
            book.asks.get(&price).cloned().unwrap_or(Decimal::ZERO)
        };

        let new_qty = current_qty + delta;

        if is_bid {
            book.update_bid(price, new_qty);
        } else {
            book.update_ask(price, new_qty);
        }
    }
}
