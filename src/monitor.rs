use std::str::FromStr;
use std::time::Duration;

use anyhow::Context;
use futures::StreamExt;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use lib::AppResult;
use rust_decimal::Decimal;
use service::{
    config::service::ConfigService,
    kalshi::service::KalshiService,
    market::symbols::Min15Symbols,
    polymarket::{
        clob::PolymarketClobService,
        gamma::PolymarketGammaService,
        ws::{PolymarketFeedUpdate, PolymarketWebSocketService},
    },
    services::ServiceProvider,
    strategy::{
        arbitrage::ArbitrageProcessor,
        events::OrderbookChangeEvent,
        orderbook::processor::OrderbookProcessor,
    },
};
use tracing::{error, info, warn};

pub struct RunMonitorParams {
    pub services: ServiceProvider,
    pub min_entry_edge_cents: f64,
}

pub async fn run_monitor(params: RunMonitorParams) -> AppResult<()> {
    info!("🚀 Initializing Monitor...");
    let services = params.services;

    // 1. Retrieve Services
    let pm_gamma = services.get_service_unchecked::<PolymarketGammaService>().await;
    let pm_ws = services.get_service_unchecked::<PolymarketWebSocketService>().await;
    let pm_clob = services.get_service_unchecked::<PolymarketClobService>().await;
    let kalshi = services.get_service_unchecked::<KalshiService>().await;
    let config_service = services.get_service_unchecked::<ConfigService>().await;

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
        pm_tokens.get(0).context("Missing Poly Asset 0")?.clone(),
        pm_tokens.get(1).context("Missing Poly Asset 1")?.clone(),
    );
    let condition_id = pm_market.condition_id.context("Missing Condition ID")?;

    // Fetch Kalshi Details
    let kalshi_event = kalshi
        .get_current_15min_market_by_symbol(symbol.clone())
        .await?;
    let kalshi_ticker = kalshi_event.markets[0].ticker.clone();

    info!(
        "✅ Markets Found:\n  - Poly: {} (Yes: {}, No: {})\n  - Kalshi: {}",
        pm_market.slug.unwrap_or_default(),
        asset_yes,
        asset_no,
        kalshi_ticker
    );

    // 3. Initialize Processors
    
    // A. Arbitrage Processor (Handles Execution)
    // We pass the token IDs so it knows how to map opportunities to execution services
    let arbitrage_processor = ArbitrageProcessor::new(
        services.clone(),
        asset_yes.clone(), // Convert String to U256
        asset_no.clone(),  // Convert String to U256
    ).await?;
    
    // B. Orderbook Processor (Handles State & Signal Detection)
    // Note: In a complete setup, you would pass `arbitrage_processor.get_sender()` 
    // into the OrderbookProcessor so it can emit signals to it.
    let orderbook_processor = OrderbookProcessor::new(
        services.clone(),
        asset_yes.clone(),
        asset_no.clone(),
        kalshi_ticker.clone(),
        edge_decimal,
    );

    // 4. Start Processors (Spawn Tasks)
    let _arb_handle = arbitrage_processor.start().await;
    let _ob_handle = orderbook_processor.start().await;
    
    // Channel to feed data into the Orderbook Processor
    let event_tx = orderbook_processor.get_event_sender();

    // 5. Subscribe to Data Streams

    // Polymarket Subscription
    let mut pm_stream = pm_ws
        .subscribe_feed(
            vec![asset_yes, asset_no],
            vec![condition_id],
        )?
        .boxed();

    // Kalshi Subscription
    // Subscribe to Orderbook Deltas (and potentially snapshots via channel logic if implemented)
    kalshi.subscribe_orderbook(kalshi_ticker.clone()).await?;

    info!("👂 Listening for Market Data...");

    // 6. Main Ingestion Loop
    loop {
        tokio::select! {
            // --- Polymarket WebSocket Events ---
            Some(msg_result) = pm_stream.next() => {
                match msg_result {
                    Ok(PolymarketFeedUpdate::BestBidAsk(bba)) => {
                        // Forward BBA updates to the Orderbook Processor
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::Polymarket(bba)).await {
                            error!("Failed to send Poly update to processor: {:?}", e);
                            break; // Channel closed, critical failure
                        }
                    }
                    Ok(PolymarketFeedUpdate::Trade(_trade)) => {
                        // Optional: Handle trades for volume analysis or fill tracking
                    }
                    Err(e) => {
                        warn!("Polymarket WS Stream Error: {:?}", e);
                        // In a production bot, implement reconnection logic here
                    }
                }
            }

            // --- Kalshi WebSocket Events ---
            msg = kalshi.ws_client.next_message() => {
                match msg {
                    Ok(KalshiSocketMessage::OrderbookDelta(delta)) => {
                        // Forward Delta updates to the Orderbook Processor
                        // The processor logic currently handles the Synthetic generation
                        if let Err(e) = event_tx.send(OrderbookChangeEvent::Kalshi(delta.msg)).await {
                            error!("Failed to send Kalshi update to processor: {:?}", e);
                            break;
                        }
                    }
                    Ok(KalshiSocketMessage::OrderbookSnapshot(_snapshot)) => {
                        // TODO: Update OrderbookProcessor to handle Snapshots (Reset Book)
                        // Currently ignored based on existing processor implementation
                        info!("Received Kalshi Snapshot (Waiting for deltas...)");
                    }
                    Err(e) => {
                        error!("Kalshi WS Error: {:?}", e);
                        // In production, trigger reconnection
                        break;
                    }
                    _ => {} // Ignore other messages (e.g. KeepAlive)
                }
            }
        }
    }

    Ok(())
}