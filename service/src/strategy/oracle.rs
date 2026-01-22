use std::time::Duration;

use futures::StreamExt;
use tokio::sync::broadcast;
use tokio::time::interval;
use tracing::{info, warn};

use crate::{
    chainlink::service::ChainLinkService, kalshi::service::KalshiService,
    market::symbols::Min15Symbols, services::ServiceProvider, strategy::events::OracleUpdateEvent,
};

pub struct MarketDataOracle {
    services: ServiceProvider,
    symbol: Min15Symbols,
    broadcaster: broadcast::Sender<OracleUpdateEvent>,
}

impl MarketDataOracle {
    pub fn new(
        services: ServiceProvider,
        symbol: Min15Symbols,
    ) -> (Self, broadcast::Receiver<OracleUpdateEvent>) {
        // Create a broadcast channel with a small buffer (we only care about recent prices)
        let (tx, rx) = broadcast::channel(100);

        (
            Self {
                services,
                symbol,
                broadcaster: tx,
            },
            rx,
        )
    }

    /// Returns a new receiver for other services to subscribe
    pub fn subscribe(&self) -> broadcast::Receiver<OracleUpdateEvent> {
        self.broadcaster.subscribe()
    }

    pub async fn run(self) {
        info!("🔮 MarketDataOracle started for {:?}", self.symbol);

        let chainlink_service = self
            .services
            .get_service_unchecked::<ChainLinkService>()
            .await;
        let kalshi_service = self.services.get_service_unchecked::<KalshiService>().await;

        // 1. Setup Chainlink Stream
        let mut chainlink_stream = chainlink_service
            .stream_chainlink_prices(self.symbol.clone())
            .boxed();

        // 2. Setup Kalshi Polling
        let mut kalshi_poll_interval = interval(Duration::from_secs(1));

        loop {
            tokio::select! {
                // --- Chainlink Updates ---
                Some(res) = chainlink_stream.next() => {
                    match res {
                        Ok(cl_update) => {
                            let price = cl_update.value;
                            // Note: You might need to adjust scaling here based on the raw value
                            // For simplicity assuming Decimal handles it or mapped correctly in service

                            let event = OracleUpdateEvent::Polymarket {
                                price,
                                timestamp: chrono::Utc::now().timestamp(),
                            };

                            if self.broadcaster.send(event).is_err() {
                                // No receivers, not a critical error
                            }
                        }
                        Err(e) => warn!("Chainlink stream error: {:?}", e),
                    }
                }

                // --- Kalshi Polling ---
                _ = kalshi_poll_interval.tick() => {
                    match kalshi_service.get_last_traded_price(self.symbol.clone()).await {
                        Ok((ts, price)) => {
                            let event = OracleUpdateEvent::Kalshi {
                                price,
                                timestamp: ts,
                            };
                            if self.broadcaster.send(event).is_err() {
                                // No receivers
                            }
                        }
                        Err(e) => warn!("Failed to poll Kalshi price: {:?}", e),
                    }
                }
            }
        }
    }
}
