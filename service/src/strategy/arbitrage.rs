use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Result;
use chrono::Utc;
use lib::AppResult;
use polymarket_client_sdk::clob::types::Side;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tokio::sync::{RwLock, mpsc};
use tokio::task::JoinHandle;
use tracing::{error, info, warn};

use crate::market::exchange::Exchange;
use crate::strategy::{PairedPosition, Position};
use crate::{
    config::service::ConfigService,
    kalshi::service::KalshiService,
    polymarket::clob::{PolymarketClobService, PolymarketExecutionService},
    services::ServiceProvider,
    strategy::{ArbSizing, Order, market::Opportunity},
};

#[derive(Debug, Clone)]
pub struct ArbitrageProcessorConfig {
    pub budget_per_trade: Decimal,
}

impl Default for ArbitrageProcessorConfig {
    fn default() -> Self {
        Self {
            budget_per_trade: Decimal::from(10),
        }
    }
}

#[derive(Clone)]
pub struct ArbitrageProcessor {
    services: ServiceProvider,

    config: ArbitrageProcessorConfig,

    pm_exec_yes: Arc<PolymarketExecutionService>,
    pm_exec_no: Arc<PolymarketExecutionService>,
    kalshi: Arc<KalshiService>,

    opportunity_rx: Arc<RwLock<Option<mpsc::Receiver<Opportunity>>>>,
    opportunity_tx: mpsc::Sender<Opportunity>,

    monitor_tx: mpsc::Sender<PairedPosition>,

    // State
    last_action: Arc<RwLock<Instant>>,
    cooldown: Duration,
}

impl ArbitrageProcessor {
    pub async fn new(
        services: ServiceProvider,
        poly_token_yes: polymarket_client_sdk::types::U256,
        poly_token_no: polymarket_client_sdk::types::U256,
        monitor_tx: mpsc::Sender<PairedPosition>,
    ) -> Result<Self> {
        let (tx, rx) = mpsc::channel(100);

        let config = services.get_service_unchecked::<ConfigService>().await;
        let pm_clob = services
            .get_service_unchecked::<PolymarketClobService>()
            .await;
        let kalshi = services.get_service_unchecked::<KalshiService>().await;

        // Initialize execution helpers for both sides (Yes/No)
        let pm_exec_yes = Arc::new(
            PolymarketExecutionService::new(pm_clob.clone(), config.clone(), poly_token_yes)
                .await?,
        );
        let pm_exec_no = Arc::new(
            PolymarketExecutionService::new(pm_clob.clone(), config.clone(), poly_token_no).await?,
        );

        let config = ArbitrageProcessorConfig::default();

        Ok(Self {
            services,
            config,
            pm_exec_yes,
            pm_exec_no,
            kalshi,
            monitor_tx,
            opportunity_rx: Arc::new(RwLock::new(Some(rx))),
            opportunity_tx: tx,
            last_action: Arc::new(RwLock::new(Instant::now())),
            cooldown: Duration::from_secs(5),
        })
    }

    pub fn get_sender(&self) -> mpsc::Sender<Opportunity> {
        self.opportunity_tx.clone()
    }

    pub async fn start(&self) -> JoinHandle<AppResult<()>> {
        let mut rx = self
            .opportunity_rx
            .write()
            .await
            .take()
            .expect("Opportunity RX already taken");

        let processor = self.clone();

        tokio::spawn(async move {
            info!("⚡ ArbitrageProcessor started");

            loop {
                match rx.recv().await {
                    Some(opp) => processor.process_opportunity(opp).await,
                    None => break,
                }
            }

            Ok(())
        })
    }

    async fn process_opportunity(&self, opp: Opportunity) {
        let mut last_action = self.last_action.write().await;
        if last_action.elapsed() < self.cooldown {
            return;
        }

        info!("⚡ Executing Arbitrage Opportunity: {:?}", opp);

        // 2. Calculate Sizing
        let budget_per_trade = self.config.budget_per_trade;
        let poly_cents = (opp.poly_price * Decimal::from(100))
            .ceil()
            .to_u32()
            .unwrap_or(100);

        let sizing =
            match ArbSizing::calculate(budget_per_trade, poly_cents, opp.kalshi_price_cents) {
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

        let poly_order_res =
            exec_service.build_market_order(poly_limit_price, quantity_dec, Side::Buy);

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
            self.kalshi.place_market_order(
                opp.kalshi_ticker.clone(),
                opp.kalshi_side.clone(),
                "buy".to_string(),
                kalshi_order
            )
        );

        // 6. Handle Results
        match (p_res, k_res) {
            (Ok(poly_resp), Ok(kalshi_resp)) => {
                info!(
                    "✅ ENTRY SUCCESSFUL: Poly TX: {:?}, Kalshi Filled",
                    poly_resp.order_id
                );
                *last_action = Instant::now();

                let pm_side = if opp.poly_token_id == self.pm_exec_yes.token_id {
                    "yes".to_string()
                } else {
                    "no".to_string()
                };

                let pm_position = Position {
                    exchange: Exchange::Polymarket,
                    order_id: poly_resp.order_id,
                    side: pm_side,
                    timestamp: Utc::now().timestamp(),
                };

                let ks_position = Position {
                    exchange: Exchange::Kalshi,
                    order_id: kalshi_resp.order.order_id,
                    side: kalshi_resp.order.side,
                    timestamp: Utc::now().timestamp(),
                };

                let paired_position = PairedPosition::new(pm_position, ks_position);

                if let Err(e) = self.monitor_tx.send(paired_position).await {
                    error!("⚠️ ERROR SENDING PAIRED POSITION: {:?}", e);
                }
            }
            (Err(e), Ok(kalshi_resp)) => {
                error!(
                    "⚠️ PARTIAL FILL ERROR: Poly Failed ({:?}) but Kalshi Executed!",
                    e
                );
                // TODO: Implement emergency exit for Kalshi leg
            }
            (Ok(poly_resp), Err(e)) => {
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
}
