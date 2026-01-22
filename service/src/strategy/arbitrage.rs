use std::str::FromStr;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::{Context, Result};
use lib::AppResult;
use polymarket_client_sdk::clob::types::Side;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use tokio::sync::{broadcast, mpsc, RwLock};
use tokio::{select, task::JoinHandle};
use tracing::{error, info, warn};

use crate::{
    config::service::ConfigService,
    kalshi::service::KalshiService,
    polymarket::clob::{PolymarketClobService, PolymarketExecutionService},
    services::{ServiceFactory, ServiceProvider},
    strategy::{market::Opportunity, ArbSizing, Order},
};

#[derive(Debug, Clone)]
pub struct ArbitrageProcessorConfig {
    pub budget_per_trade: Decimal
}

impl Default for ArbitrageProcessorConfig {
    fn default() -> Self {
        Self {
            budget_per_trade: Decimal::from(10)
        }
    }
}

#[derive(Clone)]
pub struct ArbitrageProcessor {
    services: ServiceProvider,
    
    config: ArbitrageProcessorConfig,
    
    // Services for Execution
    pm_clob: Arc<PolymarketClobService>,
    pm_exec_yes: Arc<PolymarketExecutionService>,
    pm_exec_no: Arc<PolymarketExecutionService>,
    kalshi: Arc<KalshiService>,

    // Channels
    opportunity_rx: Arc<RwLock<Option<mpsc::Receiver<Opportunity>>>>,
    opportunity_tx: mpsc::Sender<Opportunity>,
    
    shutdown_rx: Arc<RwLock<Option<broadcast::Receiver<()>>>>,
    
    // State
    last_action: Arc<RwLock<Instant>>,
    cooldown: Duration,
}

impl ArbitrageProcessor {
    pub async fn new(
        services: ServiceProvider,
        poly_token_yes: polymarket_client_sdk::types::U256,
        poly_token_no: polymarket_client_sdk::types::U256,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(100);
        
        let config = services.get_service_unchecked::<ConfigService>().await;
        let pm_clob = services.get_service_unchecked::<PolymarketClobService>().await;
        let kalshi = services.get_service_unchecked::<KalshiService>().await;

        // Initialize execution helpers for both sides (Yes/No)
        let pm_exec_yes = Arc::new(
            PolymarketExecutionService::new(pm_clob.clone(), config.clone(), poly_token_yes).await?
        );
        let pm_exec_no = Arc::new(
            PolymarketExecutionService::new(pm_clob.clone(), config.clone(), poly_token_no).await?
        );
        
        let config = ArbitrageProcessorConfig::default();

        Ok(Self {
            services,
            config,
            pm_clob,
            pm_exec_yes,
            pm_exec_no,
            kalshi,
            opportunity_rx: Arc::new(RwLock::new(Some(rx))),
            opportunity_tx: tx,
            shutdown_rx: Arc::new(RwLock::new(None)), // Should pass this in or init
            last_action: Arc::new(RwLock::new(Instant::now())),
            cooldown: Duration::from_secs(5),
        })
    }

    /// Set the shutdown receiver (call before start)
    pub async fn set_shutdown_rx(&self, rx: broadcast::Receiver<()>) {
        *self.shutdown_rx.write().await = Some(rx);
    }

    pub fn get_sender(&self) -> mpsc::Sender<Opportunity> {
        self.opportunity_tx.clone()
    }

    pub async fn start(&self) -> JoinHandle<AppResult<()>> {
        let mut rx = self.opportunity_rx.write().await.take()
            .expect("Opportunity RX already taken");
        
        // Handle optional shutdown rx gracefully
        let mut shutdown_rx_opt = self.shutdown_rx.write().await.take();

        let processor = self.clone();

        tokio::spawn(async move {
            info!("⚡ ArbitrageProcessor started");

            loop {
                // If we have a shutdown receiver, select on it. 
                // If not, just block on opportunity (useful for testing/simpler setups)
                if let Some(ref mut shutdown) = shutdown_rx_opt {
                    select! {
                        res = rx.recv() => {
                            match res {
                                Some(opp) => processor.process_opportunity(opp).await,
                                None => break,
                            }
                        }
                        _ = shutdown.recv() => {
                            info!("⚡ ArbitrageProcessor shutdown signal");
                            break;
                        }
                    }
                } else {
                    match rx.recv().await {
                        Some(opp) => processor.process_opportunity(opp).await,
                        None => break,
                    }
                }
            }
            
            Ok(())
        })
    }

    async fn process_opportunity(&self, opp: Opportunity) {
        // 1. Check Cooldown
        let mut last_action = self.last_action.write().await;
        if last_action.elapsed() < self.cooldown {
            return;
        }

        info!("⚡ Executing Arbitrage Opportunity: {:?}", opp);

        // 2. Calculate Sizing
        let budget_per_trade = self.config.budget_per_trade;
        let poly_cents = (opp.poly_price * Decimal::from(100)).ceil().to_u32().unwrap_or(100);
        
        let sizing = match ArbSizing::calculate(budget_per_trade, poly_cents, opp.kalshi_price_cents) {
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
        // Add tiny markup for FOK safety
        let poly_limit_price = opp.poly_price;
        
        let poly_order_res = exec_service.build_market_order(
            poly_limit_price, 
            quantity_dec, 
            Side::Buy
        );

        let poly_order = match poly_order_res {
            Ok(o) => o,
            Err(e) => {
                error!("Failed to build Poly order: {:?}", e);
                return;
            }
        };

        let kalshi_order = Order::new(opp.kalshi_price_cents, sizing.quantity);

        // 5. Execute in Parallel
        info!("🚀 Firing Orders: Poly Buy {} @ {}, Kalshi Buy {} @ {}c", 
            quantity_dec, poly_limit_price, sizing.quantity, opp.kalshi_price_cents);

        let (p_res, k_res) = tokio::join!(
            exec_service.execute_order(poly_order),
            self.kalshi.place_fok_order(
                opp.kalshi_ticker.clone(),
                opp.kalshi_side.clone(),
                "buy".to_string(),
                kalshi_order
            )
        );

        // 6. Handle Results
        match (p_res, k_res) {
            (Ok(poly_resp), Ok(_)) => {
                info!("✅ ENTRY SUCCESSFUL: Poly TX: {:?}, Kalshi Filled", poly_resp.order_id);
                *last_action = Instant::now(); // Update cooldown only on success attempt
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
}