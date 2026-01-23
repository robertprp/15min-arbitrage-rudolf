// File: src/monitor.rs

use std::{str::FromStr, time::Duration};

use anyhow::Context;
use futures::StreamExt;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use lib::AppResult;
use rust_decimal::Decimal;
use service::{
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
        state::MarketState,
    },
    utils::get_duration_until_next_window,
};
use tokio::time::{sleep, timeout};
use tracing::{error, info, warn};

pub struct RunMonitorParams {
    pub services: ServiceProvider,
    pub min_entry_edge_cents: f64,
}

/// The Main Entry Point: Loops forever, handling session transitions
pub async fn run_monitor(params: RunMonitorParams) -> AppResult<()> {
    info!("🔄 Starting Continuous Monitor Loop");

    loop {
        // 1. Calculate how long to run this session
        let duration_left = get_duration_until_next_window();

        // If we are extremely close to the edge (e.g. < 5 seconds),
        // wait for the next window to start to avoid setting up a dying market.
        if duration_left.as_secs() < 10 {
            info!(
                "⏳ Too close to window close ({}s). Waiting for next window...",
                duration_left.as_secs()
            );
            sleep(duration_left + Duration::from_secs(4)).await;
            continue;
        }

        info!(
            "⏱️  Starting Session. Time remaining in window: {:?}",
            duration_left
        );

        let session_result = timeout(
            duration_left,
            monitor_single_window(params.services.clone(), params.min_entry_edge_cents),
        )
        .await;

        // 3. Handle End of Window
        match session_result {
            Ok(result) => {
                // If the function returned Early (Ok), it might have crashed or finished unexpectedly
                if let Err(e) = result {
                    error!("❌ Session crashed: {:?}. Restarting in 5s...", e);
                    sleep(Duration::from_secs(5)).await;
                }
            }
            Err(_) => {
                // Timeout elapsed -> This is the expected behavior!
                info!("🏁 Window Closed. Shutting down session and reinitializing...");
                // Add a small buffer to ensure we cross the time boundary for the ticker generators
                sleep(Duration::from_secs(2)).await;
            }
        }

        // Loop repeats, creating fresh state, managers, and processors.
    }
}

/// The Logic for a Single 15-Minute Window
async fn monitor_single_window(
    services: ServiceProvider,
    min_entry_edge_cents: f64,
) -> AppResult<()> {
    let config_service = services.get_service_unchecked::<ConfigService>().await;

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
    let edge_decimal =
        Decimal::from_str(&min_entry_edge_cents.to_string()).context("Invalid edge decimal")?;

    info!("🔎 Discovering Markets for {:?}...", symbol);

    // Fetch Polymarket Details (Will fetch based on *current* timestamp)
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

    // Fetch Kalshi Details (Will fetch based on *current* timestamp)
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

    let pm_metadata = MarketMetadata::new_poly(pm_market.clone()).await?;
    let kalshi_metadata = MarketMetadata::new_kalshi(kalshi_event).await?;

    // 3. Setup Oracle
    // Note: MarketDataOracle is recreated here, so it connects to the specific chainlink/kalshi
    // feeds relevant to this session if we were to customize them per window.
    let (oracle, arb_oracle_rx) = MarketDataOracle::new(services.clone(), symbol.clone());
    let manager_oracle_rx = oracle.subscribe();

    // 4. Initialize FRESH Shared Market State
    // This wipes the orderbooks from the previous window automatically
    let market_state = MarketState::new();

    // 5. Setup Position Manager (Fresh Risk Layer & State)
    let manager = PositionManager::new(PositionManagerParams {
        config_service: config_service.clone(),
        poly_metadata: pm_metadata,
        kalshi_metadata,
        clob_service: pm_clob.clone(),
        kalshi_service: kalshi.clone(),
        oracle_rx: manager_oracle_rx,
        poly_token_yes: asset_yes,
        poly_token_no: asset_no,
        market_state: market_state.clone(),
    })
    .await?;

    let executor_tx = manager.get_sender().clone();

    sleep(Duration::from_micros(500)).await;

    // 6. Setup Orderbook Processor
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

    // 7. Spawn Tasks
    // We capture the JoinHandles (implicitly) but since we are in a timeout block in `run_monitor`,
    // when this function returns, the channels (tx/rx) will drop.
    // The spawned loops rely on channel communication; once channels break, they will exit gracefully.
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

    // 8. Subscribe to Market Data
    // Polymarket WS
    let mut pm_stream = pm_ws
        .subscribe_feed(vec![asset_yes, asset_no], vec![condition_id])?
        .boxed();

    // Kalshi WS
    kalshi.subscribe_orderbook(kalshi_market_ticker).await?;
    kalshi.subscribe_fills().await?;

    info!("👂 Listening for Market Data...");

    // 9. Main Ingestion Loop
    loop {
        tokio::select! {
            // --- Polymarket WebSocket Events ---
            Some(msg_result) = pm_stream.next() => {
                match msg_result {
                    Ok(PolymarketFeedUpdate::BestBidAsk(bba)) => {
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::Polymarket(bba)).await {
                            // If channel is closed, it means processor died or we are shutting down
                            error!("Channel closed: {:?}", e);
                            break;
                        }
                    }
                    Ok(PolymarketFeedUpdate::Trade(_trade)) => {}
                    Err(e) => warn!("Polymarket WS Stream Error: {:?}", e),
                }
            }

            // --- Kalshi WebSocket Events ---
            msg = kalshi.ws_client.next_message() => {
                match msg {
                    Ok(KalshiSocketMessage::OrderbookDelta(delta)) => {
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::KalshiDelta(delta.msg)).await {
                             error!("Channel closed: {:?}", e);
                             break;
                        }
                    }
                    Ok(KalshiSocketMessage::OrderbookSnapshot(snapshot)) => {
                        info!("📸 Received Kalshi Snapshot");
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::KalshiSnapshot(snapshot.msg)).await {
                             error!("Channel closed: {:?}", e);
                             break;
                        }
                    }
                    Err(e) => {
                        error!("Kalshi WS Error: {:?}", e);
                        // Optional: Break to trigger restart or continue
                        break;
                    }
                    _ => {}
                }
            }
        }
    }

    Ok(())
}
