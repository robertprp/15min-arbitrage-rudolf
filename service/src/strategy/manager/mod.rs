// File: service/src/strategy/manager/mod.rs

use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use polymarket_client_sdk::clob::types::Side;
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};
use std::str::FromStr;

use crate::config::service::ConfigService;
use crate::kalshi::service::KalshiService;
use crate::market::exchange::Exchange;
use crate::market::metadata::MarketMetadata;
use crate::polymarket::clob::{PolymarketClobService, PolymarketExecutionService};
use crate::strategy::events::OracleUpdateEvent;
use crate::strategy::market::Opportunity;
use crate::strategy::risk::layer::{ActionExit, RiskLayer};
use crate::strategy::{ArbSizing, Order, PairedPosition, Position};

pub struct PositionManager {
    pub positions: Vec<PairedPosition>,

    // Services
    clob_service: Arc<PolymarketClobService>,
    kalshi_service: Arc<KalshiService>,
    
    // Execution Helpers
    pm_exec_yes: Arc<PolymarketExecutionService>,
    pm_exec_no: Arc<PolymarketExecutionService>,

    // Configuration
    budget_per_trade: Decimal,

    // State
    last_action: Instant,
    cooldown: Duration,

    // Risk Management
    risk_layer: RiskLayer<ActionExit>,

    // Channels
    opportunity_rx: mpsc::Receiver<Opportunity>, // Changed from PairedPosition
    opportunity_tx: mpsc::Sender<Opportunity>,   // To hand out clones to producers
    oracle_rx: broadcast::Receiver<OracleUpdateEvent>,
}

pub struct PositionManagerParams {
    pub config_service: Arc<ConfigService>,
    pub clob_service: Arc<PolymarketClobService>,
    pub kalshi_service: Arc<KalshiService>,
    pub poly_metadata: MarketMetadata,
    pub kalshi_metadata: MarketMetadata,
    pub oracle_rx: broadcast::Receiver<OracleUpdateEvent>,
    
    // Assets for execution
    pub poly_token_yes: U256,
    pub poly_token_no: U256,
}

impl PositionManager {
    pub async fn new(params: PositionManagerParams) -> anyhow::Result<Self> {
        let (opportunity_tx, opportunity_rx) = mpsc::channel(100);

        let clob_service = params.clob_service;
        let kalshi_service = params.kalshi_service;
        let poly_metadata = params.poly_metadata;
        let kalshi_metadata = params.kalshi_metadata;
        let oracle_rx = params.oracle_rx;

        // Initialize Execution Services (Async network calls)
        let pm_exec_yes = Arc::new(
            PolymarketExecutionService::new(
                clob_service.clone(), 
                params.config_service.clone(), 
                params.poly_token_yes
            ).await?
        );

        let pm_exec_no = Arc::new(
            PolymarketExecutionService::new(
                clob_service.clone(), 
                params.config_service.clone(), 
                params.poly_token_no
            ).await?
        );

        // Initialize Exit Risk Layer
        let risk_layer = RiskLayer::new(poly_metadata, kalshi_metadata);

        Ok(Self {
            positions: Vec::new(),
            clob_service,
            kalshi_service,
            pm_exec_yes,
            pm_exec_no,
            budget_per_trade: Decimal::from(10), // Default budget
            last_action: Instant::now(),
            cooldown: Duration::from_secs(5),
            risk_layer,
            opportunity_tx,
            opportunity_rx,
            oracle_rx,
        })
    }

    pub fn get_sender(&self) -> mpsc::Sender<Opportunity> {
        self.opportunity_tx.clone()
    }

    pub async fn start(mut self) {
        info!("👮 Position Manager Started");

        loop {
            tokio::select! {
                // 1. Handle New Opportunities (Execute Arbitrage)
                Some(opp) = self.opportunity_rx.recv() => {
                    self.handle_opportunity(opp).await;
                }

                // 2. Handle Oracle Updates (Update Risk Layer)
                Ok(event) = self.oracle_rx.recv() => {
                    match event {
                        OracleUpdateEvent::Polymarket { price, .. } => {
                            self.risk_layer.update_poly_price(price);
                        }
                        OracleUpdateEvent::Kalshi { price, .. } => {
                            self.risk_layer.update_kalshi_price(price);
                        }
                    }

                    // 3. Check Risk Conditions immediately after price update
                    self.check_risk_unwind().await;
                }
            }
        }
    }

    /// Logic moved from ArbitrageProcessor
    async fn handle_opportunity(&mut self, opp: Opportunity) {
        // 1. Check Cooldown
        if self.last_action.elapsed() < self.cooldown {
            return;
        }

        info!("⚡ Executing Arbitrage Opportunity: {:?}", opp);

        // 2. Calculate Sizing
        let poly_cents = (opp.poly_price * Decimal::from(100))
            .ceil()
            .to_u32()
            .unwrap_or(100);

        let sizing = match ArbSizing::calculate(self.budget_per_trade, poly_cents, opp.kalshi_price_cents) {
            Some(s) => s,
            None => {
                warn!("Could not calculate sizing for opportunity");
                return;
            }
        };

        // 3. Determine correct Poly Execution Service
        let exec_service = if opp.poly_token_id == self.pm_exec_yes.token_id {
            &self.pm_exec_yes
        } else {
            &self.pm_exec_no
        };

        // 4. Build Orders
        let quantity_dec = Decimal::from(sizing.quantity);
        
        let poly_limit_price = opp.poly_price + Decimal::from_str("0.02").unwrap_or_default();

        info!("Buying Poly at price {} and amount of shares {}", poly_limit_price, quantity_dec);
        info!("Buying Kalshi at price {} and amount of shares {}", opp.kalshi_price_cents, quantity_dec);

        let poly_order_res = exec_service.build_market_order_with_shares(poly_limit_price, quantity_dec, Side::Buy);

        let poly_order = match poly_order_res {
            Ok(o) => o,
            Err(e) => {
                error!("Failed to build Poly order: {:?}", e);
                return;
            }
        };

        let kalshi_order = Order::new(opp.kalshi_price_cents, sizing.quantity);

        // 5. Execute in Parallel
        info!(
            "🚀 Firing Orders: Poly Buy {} @ {}, Kalshi Buy {} @ {}c",
            quantity_dec, poly_limit_price, sizing.quantity, opp.kalshi_price_cents
        );

        let (p_res, k_res) = tokio::join!(
            exec_service.execute_order(poly_order),
            self.kalshi_service.place_market_order(
                opp.kalshi_ticker.clone(),
                opp.kalshi_side.clone(),
                "buy".to_string(),
                kalshi_order
            )
        );

        // 6. Handle Results & Track Position
        match (p_res, k_res) {
            (Ok(poly_resp), Ok(kalshi_resp)) => {
                info!(
                    "✅ ENTRY SUCCESSFUL: Poly TX: {:?}, Kalshi Filled",
                    poly_resp.order_id
                );
                
                // Update State
                self.last_action = Instant::now();

                let pm_side = if opp.poly_token_id == self.pm_exec_yes.token_id {
                    "yes".to_string()
                } else {
                    "no".to_string()
                };

                let pm_position = Position {
                    exchange: Exchange::Polymarket,
                    order_id: poly_resp.order_id,
                    side: pm_side,
                    timestamp: chrono::Utc::now().timestamp(),
                };

                let ks_position = Position {
                    exchange: Exchange::Kalshi,
                    order_id: kalshi_resp.order.order_id,
                    side: kalshi_resp.order.side,
                    timestamp: chrono::Utc::now().timestamp(),
                };

                let paired_position = PairedPosition::new(pm_position, ks_position);
                
                // Track internally
                self.positions.push(paired_position);
                info!("✅ Position Manager tracked new position. Total: {}", self.positions.len());
            }
            (Err(e), Ok(_)) => {
                error!("⚠️ PARTIAL FILL ERROR: Poly Failed ({:?}) but Kalshi Executed!", e);
                // TODO: Implement emergency exit for Kalshi leg
            }
            (Ok(_), Err(e)) => {
                error!("⚠️ PARTIAL FILL ERROR: Kalshi Failed ({:?}) but Poly Executed!", e);
                // TODO: Implement emergency exit for Poly leg
            }
            (Err(ep), Err(ek)) => {
                error!("❌ ENTRY FAILED: Poly: {:?}, Kalshi: {:?}", ep, ek);
            }
        }
    }

    async fn check_risk_unwind(&self) {
        if self.positions.is_empty() {
            return;
        }

        if self.risk_layer.should_unwind() {
            warn!("🚨 RISK SIGNAL: UNWIND TRIGGERED!");
            
            // Logic placeholder:
            // Iterate self.positions and unwind them
            info!(
                "Aggressive Unwind Logic would execute here for {} positions.",
                self.positions.len()
            );
        }
    }
}