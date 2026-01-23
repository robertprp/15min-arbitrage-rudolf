// File: src/monitor.rs

use std::{str::FromStr, time::Duration};

use anyhow::Context;
use futures::StreamExt;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use lib::AppResult;
use rust_decimal::Decimal;
use service::{
    chainlink::service::ChainLinkService,
    config::service::ConfigService,
    kalshi::service::KalshiService,
    market::{metadata::MarketMetadata, symbols::Min15Symbols},
    polymarket::{
        clob::PolymarketClobService,
        gamma::PolymarketGammaService,
        ws::{PolymarketFeedUpdate, PolymarketWebSocketService},
    },
    services::ServiceProvider,
    strategy::{
        events::OrderbookChangeEvent,
        manager::{PositionManager, PositionManagerParams},
        oracle::MarketDataOracle,
        orderbook::processor::OrderbookProcessor,
        state::MarketState, // <--- 1. Added Import
    },
};
use tokio::time::sleep;
use tracing::{error, info, warn};

pub struct RunMonitorParams {
    pub services: ServiceProvider,
    pub min_entry_edge_cents: f64,
}

pub async fn run_monitor(params: RunMonitorParams) -> AppResult<()> {
    info!("🚀 Initializing Monitor...");
    let services = params.services.clone();
    let config_service = services.get_service_unchecked::<ConfigService>().await;

    let chainlink_service = services.get_service_unchecked::<ChainLinkService>().await;

    // 1. Retrieve Services
    let pm_gamma = services
        .get_service_unchecked::<PolymarketGammaService>()
        .await;
    let pm_ws = services
        .get_service_unchecked::<PolymarketWebSocketService>()
        .await;
    let pm_clob = services
        .get_service_unchecked::<PolymarketClobService>()
        .await;
    let kalshi = services.get_service_unchecked::<KalshiService>().await;

    // 2. Configuration & Market Discovery
    let symbol = Min15Symbols::BTC;
    let edge_decimal = Decimal::from_str(&params.min_entry_edge_cents.to_string())
        .context("Invalid edge decimal")?;

    info!("🔎 Discovering Markets for {:?}...", symbol);

    // Fetch Polymarket Details
    let pm_market = pm_gamma
        .get_current_15min_market_by_symbol(symbol.clone())
        .await?;

    let pm_tokens = pm_market
        .clob_token_ids
        .as_ref()
        .ok_or_else(|| anyhow::anyhow!("No CLOB tokens found for Polymarket event"))?;

    let (asset_yes, asset_no) = (
        *pm_tokens.first().context("Missing Poly Asset 0")?,
        *pm_tokens.get(1).context("Missing Poly Asset 1")?,
    );
    let condition_id = pm_market.condition_id.context("Missing Condition ID")?;

    // Fetch Kalshi Details
    let kalshi_event = kalshi
        .get_current_15min_market_by_symbol(symbol.clone())
        .await?;

    let kalshi_event_ticker = kalshi_event.event.event_ticker.clone();
    let kalshi_market_ticker = kalshi_event.markets[0].ticker.clone();

    info!(
        "✅ Markets Found:\n  - Poly: {} (Yes: {}, No: {})\n  - Kalshi: {}",
        pm_market.slug.clone().unwrap_or_default(),
        asset_yes,
        asset_no,
        kalshi_event_ticker
    );

    let (_ts, strike_price) = chainlink_service.get_last_15min_open().await?;
    let pm_metadata = MarketMetadata::new_poly(pm_market.clone(), strike_price)?;
    let kalshi_metadata = MarketMetadata::new_kalshi(kalshi_event)?;

    // 3. Setup Oracle
    let (oracle, arb_oracle_rx) = MarketDataOracle::new(services.clone(), symbol.clone());
    let manager_oracle_rx = oracle.subscribe();

    // 3b. Initialize Shared Market State
    let market_state = MarketState::new(); // <--- 2. Initialize State

    // 4. Setup Position Manager (Now handles Execution)
    let manager = PositionManager::new(PositionManagerParams {
        config_service: config_service.clone(),
        poly_metadata: pm_metadata,
        kalshi_metadata,
        clob_service: pm_clob.clone(),
        kalshi_service: kalshi.clone(),
        oracle_rx: manager_oracle_rx,
        poly_token_yes: asset_yes,
        poly_token_no: asset_no,
        market_state: market_state.clone(), // <--- 3. Fix: Pass MarketState
    })
    .await?;

    let executor_tx = manager.get_sender().clone();
    
    sleep(Duration::from_micros(500)).await;

    // 5. Setup Orderbook Processor (Feeds the Manager)
    let mut orderbook_processor = OrderbookProcessor::new(
        services.clone(),
        asset_yes,
        asset_no,
        pm_gamma.get_slug_by_symbol(symbol.clone()),
        kalshi_event_ticker.clone(),
        kalshi_market_ticker.clone(),
        edge_decimal,
        market_state, 
        executor_tx,
        arb_oracle_rx,
    )
    .await?;

    // 6. Spawn Tasks
    tokio::spawn(async move {
        oracle.run().await;
    });

    tokio::spawn(async move {
        manager.start().await;
    });

    sleep(Duration::from_secs(1)).await;

    let event_tx = orderbook_processor.get_event_sender();

    tokio::spawn(async move {
        if let Err(e) = orderbook_processor.start().await {
            error!("Orderbook Processor Error: {}", e);
        }
    });

    sleep(Duration::from_secs(1)).await;
    // 7. Subscribe to Market Data
    let mut pm_stream = pm_ws
        .subscribe_feed(vec![asset_yes, asset_no], vec![condition_id])?
        .boxed();

    kalshi.subscribe_orderbook(kalshi_market_ticker).await?;
    kalshi.subscribe_fills().await?;

    info!("👂 Listening for Market Data...");

    // 8. Main Ingestion Loop
    loop {
        tokio::select! {
            // --- Polymarket WebSocket Events ---
            Some(msg_result) = pm_stream.next() => {
                match msg_result {
                    Ok(PolymarketFeedUpdate::BestBidAsk(bba)) => {
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::Polymarket(bba)).await {
                            error!("Failed to send Poly update to processor: {:?}", e);
                            break;
                        }
                    }
                    Ok(PolymarketFeedUpdate::Trade(_trade)) => {
                        // Optional: Handle trades
                    }
                    Err(e) => {
                        warn!("Polymarket WS Stream Error: {:?}", e);
                        // Implement reconnection logic in production
                    }
                }
            }

            // --- Kalshi WebSocket Events ---
            msg = kalshi.ws_client.next_message() => {
                match msg {
                    Ok(KalshiSocketMessage::OrderbookDelta(delta)) => {
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::KalshiDelta(delta.msg)).await {
                            error!("Failed to send Kalshi update to processor: {:?}", e);
                            break;
                        }
                    }
                    Ok(KalshiSocketMessage::OrderbookSnapshot(snapshot)) => {
                        info!("📸 Received Kalshi Snapshot for ticker: {}", snapshot.msg.market_ticker);
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::KalshiSnapshot(snapshot.msg)).await {
                            error!("Failed to send Kalshi snapshot to processor: {:?}", e);
                            break;
                        }
                    }
                    Err(e) => {
                        error!("Kalshi WS Error: {:?}", e);
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}