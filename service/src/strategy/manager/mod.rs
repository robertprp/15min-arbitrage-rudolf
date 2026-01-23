use std::sync::Arc;
use anyhow::Context;
use tokio::sync::{broadcast, mpsc};
use tracing::{info, warn};

use crate::kalshi::service::KalshiService;
use crate::market::metadata::MarketMetadata;
use crate::polymarket::clob::PolymarketClobService;
use crate::strategy::{PairedPosition, Position};
use crate::strategy::events::OracleUpdateEvent;
use crate::strategy::risk::layer::{ActionExit, RiskLayer}; // Import Risk types

pub struct PositionManager {
    pub positions: Vec<PairedPosition>,
    
    clob_service: Arc<PolymarketClobService>,
    kalshi_service: Arc<KalshiService>,

    // Risk Management
    risk_layer: RiskLayer<ActionExit>,

    // Channels
    manager_rx: mpsc::Receiver<PairedPosition>,
    manager_tx: mpsc::Sender<PairedPosition>,
    oracle_rx: broadcast::Receiver<OracleUpdateEvent>,
}

pub struct PositionManagerParams {
    pub clob_service: Arc<PolymarketClobService>,
    pub kalshi_service: Arc<KalshiService>,
    pub poly_metadata: MarketMetadata,
    pub kalshi_metadata: MarketMetadata,
    pub oracle_rx: broadcast::Receiver<OracleUpdateEvent>,
}

impl PositionManager {
    pub fn new(params: PositionManagerParams) -> Self {
        let (manager_tx, manager_rx) = mpsc::channel(30);

        let clob_service = params.clob_service;
        let kalshi_service = params.kalshi_service;
        let poly_metadata = params.poly_metadata;
        let kalshi_metadata = params.kalshi_metadata;
        let oracle_rx = params.oracle_rx;

        // Initialize Exit Risk Layer
        let risk_layer = RiskLayer::new(poly_metadata, kalshi_metadata);

        Self {
            positions: Vec::new(),
            clob_service,
            kalshi_service,
            risk_layer,
            manager_tx,
            manager_rx,
            oracle_rx,
        }
    }

    pub fn get_sender(&self) -> mpsc::Sender<PairedPosition> {
        self.manager_tx.clone()
    }

    pub async fn start(mut self) {
        loop {
            tokio::select! {
                // 1. Handle New Positions
                Some(position) = self.manager_rx.recv() => {
                    info!("✅ Position Manager tracked new position");
                    self.positions.push(position);
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

    async fn check_risk_unwind(&self) {
        // If we have no positions, we don't care about unwinding
        if self.positions.is_empty() {
            return;
        }

        // Check the Risk Layer logic
        if self.risk_layer.should_unwind() {
            warn!("🚨 RISK SIGNAL: UNWIND TRIGGERED!");

            for position in &self.positions.clone() {
                let kalshi_position = match position.get_kalshi() {
                    Some(k) => k,
                    None => {
                        warn!(?position, "Kalshi position not found for position");
                        continue;
                    }
                };
                
                let polymarket_position = match position.get_poly() {
                    Some(p) => p,
                    None => {
                        warn!(?position, "Polymarket position not found for position");
                        continue;
                    }
                };

                info!("Unwind kalshi position: {:?}", kalshi_position);
                info!("Unwind polymarket position: {:?}", polymarket_position);

                // Execute Unwind Logic
                // self.execute_unwind_logic(kalshi_position, polymarket_position).await;
            }
            
            // For now, we just log:
            info!(
                "Aggressive Unwind Logic would execute here for {} positions.",
                self.positions.len()
            );
        }
    }
    
//     async fn execute_unwind_logic(&self, kalshi_position: &Position, polymarket_position: &Position) {
//         let k_side = kalshi_position.side.clone();
//         let k_action = "sell".to_string();
//         let ticker = kalshi_position.order_id;
//         let k_market_order = self.kalshi_service.place_market_order(ticker, k_side, k_action, order)
//     }
}
