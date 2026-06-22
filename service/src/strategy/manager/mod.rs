// File: service/src/strategy/manager/mod.rs

use std::sync::Arc;
use std::time::{Duration, Instant};

<<<<<<< Updated upstream
use polymarket_client_sdk::clob::types::Side;
=======
use anyhow::bail;
use kalshi_rs::websocket::models::OrderbookDelta;
use polymarket_client_sdk::clob::types::{OrderStatusType, Side};
>>>>>>> Stashed changes
use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use std::str::FromStr;
use tokio::sync::{broadcast, mpsc};
use tracing::{error, info, warn};

use crate::config::service::ConfigService;
use crate::kalshi::service::KalshiService;
use crate::market::exchange::{KalshiExchange, PolymarketExchange};
use crate::market::metadata::MarketMetadata;
use crate::polymarket::clob::{PolymarketClobService, PolymarketExecutionService};
use crate::strategy::events::OracleUpdateEvent;
use crate::strategy::market::Opportunity;
use crate::strategy::risk::layer::{ActionExit, RiskLayer};
use crate::strategy::state::MarketState;
use crate::strategy::{
    ArbSizing, KalshiPosition, Order, PairedPosition, PolymarketPosition, PositionProvider,
};

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

    pub market_state: MarketState,

    // Risk Management
    risk_layer: RiskLayer<ActionExit>,

    // Channels
    opportunity_rx: mpsc::Receiver<Opportunity>,
    opportunity_tx: mpsc::Sender<Opportunity>,
    oracle_rx: broadcast::Receiver<OracleUpdateEvent>,
}

pub struct PositionManagerParams {
    pub config_service: Arc<ConfigService>,
    pub clob_service: Arc<PolymarketClobService>,
    pub kalshi_service: Arc<KalshiService>,
    pub poly_metadata: MarketMetadata<PolymarketExchange>,
    pub kalshi_metadata: MarketMetadata<KalshiExchange>,
    pub oracle_rx: broadcast::Receiver<OracleUpdateEvent>,

    pub market_state: MarketState,
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
                params.poly_token_yes,
            )
            .await?,
        );

        let pm_exec_no = Arc::new(
            PolymarketExecutionService::new(
                clob_service.clone(),
                params.config_service.clone(),
                params.poly_token_no,
            )
            .await?,
        );

        // Initialize Exit Risk Layer
        let risk_layer = RiskLayer::new(poly_metadata, kalshi_metadata);

        Ok(Self {
            positions: Vec::new(),
            clob_service,
            kalshi_service,
            pm_exec_yes,
            pm_exec_no,
            market_state: params.market_state,
            budget_per_trade: Decimal::from(150), // Default budget
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

        let sizing =
            match ArbSizing::calculate(self.budget_per_trade, poly_cents, opp.kalshi_price_cents) {
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
        let poly_shares = Decimal::from(sizing.quantity);
        let mut poly_limit_price = opp.poly_price + Decimal::from_str("0.16").unwrap();
        
        if poly_limit_price >= Decimal::ONE {
            poly_limit_price = Decimal::ONE - Decimal::from_str("0.01").unwrap();
        }

<<<<<<< Updated upstream
        // TODO: Check for this, if we should do this or use FOK orders?
        let poly_limit_price = opp.poly_price + Decimal::from_str("0.05").unwrap_or_default();

        info!(
            "Buying Poly at price {} and amount of shares {}",
            poly_limit_price, quantity_dec
        );
        info!(
            "Buying Kalshi at price {} and amount of shares {}",
            opp.kalshi_price_cents, quantity_dec
        );

        let poly_order_res =
            exec_service.build_market_order_with_shares(poly_limit_price, quantity_dec, Side::Buy);

        let poly_order = match poly_order_res {
            Ok(o) => o,
            Err(e) => {
                error!("Failed to build Poly order: {:?}", e);
                return;
=======
        let poly_order = match exec_service.build_market_order_with_shares(poly_limit_price, poly_shares, Side::Buy) {
            Ok(order) => order,
            Err(err) => {
                error!("Failed to build Poly order: {}", err);
                return
>>>>>>> Stashed changes
            }
        };

        let kalshi_order = Order::new(opp.kalshi_price_cents, sizing.quantity);

        // 5. Execute in Parallel
        info!(
            "🚀 Firing Orders: Poly Buy {}: @ {}, Kalshi Buy {} @ {}c. Polymarket limit price: {} c",
            poly_shares, opp.poly_price, sizing.quantity, opp.kalshi_price_cents, poly_limit_price
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
<<<<<<< Updated upstream
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

                // Create Specific Positions
                let pm_position = PolymarketPosition {
                    order_id: poly_resp.order_id,
                    token_id: opp.poly_token_id,
                    side: pm_side,
                    shares: quantity_dec,
                    price: opp.poly_price,
                    timestamp: chrono::Utc::now().timestamp(),
                };

                let ks_position = KalshiPosition {
                    order_id: kalshi_resp.order.order_id,
                    ticker: opp.kalshi_ticker.clone(),
                    side: kalshi_resp.order.side,
                    shares: sizing.quantity,
                    price_cents: opp.kalshi_price_cents,
                    timestamp: chrono::Utc::now().timestamp(),
                };

                let paired_position = PairedPosition::new(pm_position, ks_position);

                // Track internally
                self.positions.push(paired_position);
                info!(
                    "✅ Position Manager tracked new position. Total: {}",
                    self.positions.len()
                );
            }
=======
                    (Ok(poly_resp), Ok(kalshi_resp)) => {
                        self.last_action = Instant::now();
                        info!(
                            "✅ ORDERS PLACED: Poly ID: {:?}, Kalshi ID: {:?}",
                            poly_resp.order_id, kalshi_resp.order.order_id
                        );
 
                        let mut poly_confirmed = false;
                        let mut attempts = 0;
                        let max_attempts = 10; 
 
                        let mut order_data = None;
                        
                        info!("⏳ Polling Polymarket for Match confirmation...");
 
                        while attempts < max_attempts {
                            match self.clob_service.get_order(&poly_resp.order_id).await {
                                Ok(data) => {
                                    match data.status {
                                        OrderStatusType::Matched => {
                                            info!(?order_data, "✅ Polymarket Order Matched!");
                                            order_data = Some(data);
                                            poly_confirmed = true;
                                            break;
                                        }
                                        OrderStatusType::Canceled | OrderStatusType::Unmatched => {
                                            warn!("⚠️ Polymarket Order ended with status: {:?}", data.status);
                                            break;
                                        }
                                        _ => {
                                            // Live or Delayed, wait and retry
                                        }
                                    }
                                }
                                Err(e) => {
                                    warn!("Failed to fetch Poly order status (attempt {}): {:?}", attempts, e);
                                }
                            }
 
                            tokio::time::sleep(Duration::from_secs(2)).await;
                            attempts += 1;
                        }
 
                        if !poly_confirmed {
                            error!(
                                "🚨 CRITICAL: Kalshi Filled, but Polymarket failed to Match. OrderID: {}", 
                                poly_resp.order_id
                            );
                            return;
                        }
                         
                        // Update State
                        let pm_side = if opp.poly_token_id == self.pm_exec_yes.token_id {
                            "yes".to_string()
                        } else {
                            "no".to_string()
                        };
                        
                        let order_data = order_data.unwrap();
                        
                        let shares = order_data.size_matched;
                        info!("Polymarket shares: {}", shares);
                        // info!("Polymarket positions: {:?}", positions);
                        
                        // Create Specific Positions
                        let pm_position = PolymarketPosition {
                            order_id: poly_resp.order_id,
                            token_id: opp.poly_token_id,
                            side: pm_side,
                            // shares: quantity_dec,
                            shares: shares.round_dp(2),
                            price: opp.poly_price,
                            timestamp: chrono::Utc::now().timestamp(),
                        };
 
                        let ks_position = KalshiPosition {
                            order_id: kalshi_resp.order.order_id,
                            ticker: opp.kalshi_ticker.clone(),
                            side: kalshi_resp.order.side,
                            shares: sizing.quantity,
                            price_cents: opp.kalshi_price_cents,
                            timestamp: chrono::Utc::now().timestamp(),
                        };
 
                        let paired_position = PairedPosition::new(pm_position, ks_position);
 
                        // Track internally
                        self.positions.push(paired_position);
                    }
>>>>>>> Stashed changes
            (Err(e), Ok(_)) => {
                error!(
                    "⚠️ PARTIAL FILL ERROR: Poly Failed ({:?}) but Kalshi Executed!",
                    e
                );
                // TODO: Implement emergency exit for Kalshi leg
            }
            (Ok(_), Err(e)) => {
                error!(
                    "⚠️ PARTIAL FILL ERROR: Kalshi Failed ({:?}) but Poly Executed!",
                    e
                );
                // TODO: Implement emergency exit for Poly leg
            }
            (Err(ep), Err(ek)) => {
                error!("❌ ENTRY FAILED: Poly: {:?}, Kalshi: {:?}", ep, ek);
            }
        }
    }

    pub async fn check_risk_unwind(&mut self) {
<<<<<<< Updated upstream
        if self.positions.is_empty() {
            return;
=======
            if self.positions.is_empty() {
                return;
            }
    
            // Determine Strategy based on time/risk
            let recommendation = self.risk_layer.check_exit_strategy();
    
            match recommendation {
                ExitRecommendation::Hold => {
                    // Do nothing
                }
                ExitRecommendation::PanicExit => {
                    warn!("🚨 EXECUTING PANIC UNWIND");
                    // self.unwind_all_positions().await;
                }
                ExitRecommendation::ExitWithProfit(min_profit_pct) => {
                    // Attempt to exit if we can secure > min_profit_pct
                    self.attempt_soft_unwind(min_profit_pct).await;
                }
            }
>>>>>>> Stashed changes
        }

<<<<<<< Updated upstream
        if self.risk_layer.should_unwind() {
            warn!("🚨 RISK SIGNAL: EXECUTING UNWIND SEQUENCE");
            self.unwind_all_positions().await;
        }
    }

    async fn unwind_all_positions(&mut self) {
        // 1. Snapshot the market state (Read Lock)
        let (pm_yes_bid, pm_no_bid, k_yes_bid, k_no_bid) = {
            let state = self.market_state.read().await;
            (
                state.pm_yes.best_ask(),
                state.pm_no.best_ask(),
                state.k_yes.best_ask(),
                state.k_no.best_ask(),
            )
        };

        // 2. Iterate and Execute
        let active_positions = std::mem::take(&mut self.positions);

        for position in active_positions.iter() {
            // --- Unwind Polymarket Leg ---
            let poly_pos = &position.polymarket;
            let exec_service = if poly_pos.get_side() == "yes" {
                &self.pm_exec_yes
            } else {
                &self.pm_exec_no
            };

            // If we own YES, we sell into the Best Bid of YES
            let best_bid = if poly_pos.get_side() == "yes" {
                pm_yes_bid
            } else {
                pm_no_bid
            };

            if let Some(price) = best_bid {
                let price = price + Decimal::from_str("0.02").unwrap();
                let shares_to_sell = poly_pos.get_shares();

                info!(
                    "Selling Poly {} shares of {} @ Market (Best Bid: {})",
                    shares_to_sell,
                    poly_pos.get_side(),
                    price
                );

                let order_res = exec_service.build_market_order_with_shares(
                    price,
                    shares_to_sell,
                    polymarket_client_sdk::clob::types::Side::Sell,
                );

                if let Ok(order) = order_res {
                    let _ = exec_service.execute_order(order).await;
                }
            } else {
                error!("No liquidity to unwind Poly {}", poly_pos.get_side());
            }

            // --- Unwind Kalshi Leg ---
            let kalshi_pos = &position.kalshi;
            let best_bid = if kalshi_pos.get_side() == "yes" {
                k_yes_bid
            } else {
                k_no_bid
            };

            if let Some(price) = best_bid {
                // Convert Decimal price back to cents
                let price_cents = (price * Decimal::from(100)).to_u32().unwrap_or(1);
                // Convert Decimal shares back to u32 for Kalshi Order
                let shares_to_sell = kalshi_pos.get_shares().to_u32().unwrap_or(0);

                info!(
                    "Selling Kalshi {} shares of {} @ {}c",
                    shares_to_sell,
                    kalshi_pos.get_side(),
                    price_cents
                );

                if shares_to_sell > 0 {
                    let order = Order::new(price_cents, shares_to_sell);

                    // We use the ticker stored in the position to sell the correct market
                    let _ = self
                        .kalshi_service
                        .place_market_order(
                            kalshi_pos.ticker.clone(),
                            kalshi_pos.get_side(),
                            "sell".to_string(),
                            order,
                        )
                        .await;
                }
            } else {
                error!("No liquidity to unwind Kalshi {}", kalshi_pos.get_side());
            }
        }

        // 3. Clear positions
        self.positions.clear();
        info!("✅ Unwind sequence complete.");
    }
}
=======
                        if profit_pct >= min_profit_pct {
                            info!(
                                "💰 Soft Exit Triggered: Entry {}, Current {}, Profit {:.2}% (Target {:.2}%)", 
                                total_entry_cost, total_exit_val, profit_pct * Decimal::from(100), min_profit_pct * Decimal::from(100)
                            );
                            should_sell = true;
                        }
                    }
                }
    
                if should_sell {
                    positions_to_unwind.push(pos);
                } else {
                    positions_to_keep.push(pos);
                }
            }
    
            // 3. Restore kept positions
            self.positions = positions_to_keep;
    
            // // 4. Execute Unwinds
            // if !positions_to_unwind.is_empty() {
            //      self.execute_batch_unwind(positions_to_unwind, &state).await;
            // }
        }
    
        // async fn unwind_all_positions(&mut self) {
        //     let state = self.market_state.read().await;
        //     let all_positions = std::mem::take(&mut self.positions);
        //     self.execute_batch_unwind(all_positions, &state).await;
        // }
    
        // async fn execute_batch_unwind(
        //         &self,
        //         positions: Vec<crate::strategy::PairedPosition>,
        //         state: &crate::strategy::state::MarketStateInner,
        //     ) {
        //         // 1. Snapshot prices (Cheap copy of Decimals)
        //         let (pm_yes_bid, pm_no_bid, k_yes_bid, k_no_bid) = (
        //             state.pm_yes.best_bid(),
        //             state.pm_no.best_bid(),
        //             state.k_yes.best_bid(),
        //             state.k_no.best_bid(),
        //         );
        
        //         let count_to_remove = positions.len();

        //         // 2. Map positions into a list of async tasks
        //         let tasks = positions.into_iter().map(|position| {
        //             let pm_exec_yes = self.pm_exec_yes.clone();
        //             let pm_exec_no = self.pm_exec_no.clone();
        //             let kalshi_service = self.kalshi_service.clone();
        //             let (pm_yes_bid, pm_no_bid) = (pm_yes_bid, pm_no_bid);
        //             let (k_yes_bid, k_no_bid) = (k_yes_bid, k_no_bid);
        
        //             async move {
        //                 // Prepare Poly Leg Future
        //                 let poly_future = async {
        //                     let poly_pos = &position.polymarket;
        //                     let pm_side = poly_pos.get_side();
        //                     let shares = poly_pos.get_shares();
                            
        //                     let (exec_service, best_bid) = if pm_side == "yes" {
        //                         (&pm_exec_yes, pm_yes_bid)
        //                     } else {
        //                         (&pm_exec_no, pm_no_bid)
        //                     };
        
        //                     if let Some(price) = best_bid {
        //                         // Aggressive Limit
        //                         let limit_price = price;
                                
        //                         let order_res = exec_service.build_market_order_with_shares(
        //                             limit_price,
        //                             shares,
        //                             polymarket_client_sdk::clob::types::Side::Sell,
        //                         );
        
        //                         match order_res {
        //                             Ok(order) => {
        //                                 match exec_service.execute_order(order).await {
        //                                     Ok(_) => info!("✅ Poly Unwind OK ({})", pm_side),
        //                                     Err(e) => error!("❌ Poly Exec Failed: {:?}", e),
        //                                 }
        //                             }
        //                             Err(e) => error!("❌ Poly Build Failed: {:?}", e),
        //                         }
        //                     } else {
        //                         error!("⚠️ No Poly Liquidity for {}", pm_side);
        //                     }
        //                 };
        
        //                 // Prepare Kalshi Leg Future
        //                 let kalshi_future = async {
        //                     let kalshi_pos = &position.kalshi;
        //                     let k_side = kalshi_pos.get_side();
                            
        //                     info!("Unwindding kalshi on side {} with shares {}", k_side, kalshi_pos.get_shares());
                            
        //                     let shares_u32 = kalshi_pos.get_shares().to_u32().unwrap_or(kalshi_pos.shares);
                            
        //                     let best_k_bid = if k_side == "yes" { k_yes_bid } else { k_no_bid };
        
        //                     if let Some(price) = best_k_bid {
        //                         let price_cents = (price * Decimal::from(100)).to_u32().unwrap_or(1);
                                
        //                         if shares_u32 > 0 {
        //                             let order = crate::strategy::Order::new(price_cents, shares_u32);
        //                             match kalshi_service.place_market_order(
        //                                 kalshi_pos.ticker.clone(),
        //                                 k_side.clone(),
        //                                 "sell".to_string(),
        //                                 order
        //                             ).await {
        //                                 Ok(_) => info!("✅ Kalshi Unwind OK ({})", k_side),
        //                                 Err(e) => error!("❌ Kalshi Exec Failed: {:?}", e),
        //                             }
        //                         }
        //                     } else {
        //                          error!("⚠️ No Kalshi Liquidity for {}", k_side);
        //                     }
        //                 };
        
        //                 // 3. Fire both legs of this position concurrently
        //                 tokio::join!(poly_future, kalshi_future);
        //             }
        //         });
        
        //         // 4. Fire all positions concurrently
        //         futures::future::join_all(tasks).await;

        //         // 5. Signal Decrement to Processor
        //         for _ in 0..count_to_remove {
        //             if let Err(e) = self.decrement_tx.send(()).await {
        //                 error!("Failed to signal decrement to processor: {}", e);
        //             }
        //         }
        
        //         info!("✅ Batch unwind sequence complete.");
        //     }
}
>>>>>>> Stashed changes
