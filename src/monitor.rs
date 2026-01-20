use std::str::FromStr;

use futures::StreamExt;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use lib::AppResult;
use rust_decimal::Decimal;
use service::{
    kalshi::service::KalshiService,
    market::Min15Symbols,
    polymarket::{gamma::PolymarketGammaService, ws::PolymarketWebSocketService},
    services::ServiceProvider, strategy::market::DualExchangeMarket,
};
use tracing::{error, info, warn};

pub async fn run_monitor(services: ServiceProvider, edge: f64) -> AppResult<()> {
    // 1. Get Services
    let pm_gamma = services
        .get_service_unchecked::<PolymarketGammaService>()
        .await;
    let pm_ws = services
        .get_service_unchecked::<PolymarketWebSocketService>()
        .await;
    let kalshi = services.get_service_unchecked::<KalshiService>().await;

    let symbol = Min15Symbols::BTC;
    info!("Starting 15min Market Monitor for: {:?}", symbol);

    // -------------------------------------------------------------------------
    // 2. Polymarket Setup
    // -------------------------------------------------------------------------
    info!("Fetching Polymarket details...");
    let pm_market = pm_gamma
        .get_current_15min_market_by_symbol(symbol.clone())
        .await?;

    let pm_tokens = pm_market
        .clob_token_ids
        .ok_or_else(|| anyhow::anyhow!("Polymarket market missing CLOB token IDs"))?;

    let asset_id_yes = pm_tokens.get(0).unwrap();
    let asset_id_no = pm_tokens.get(1).unwrap();

    info!(
        "Polymarket Found: {} (YES: {}, NO: {})",
        pm_market.slug.unwrap_or_default(),
        asset_id_yes,
        asset_id_no
    );

    // Subscribe to Polymarket
    let pm_assets = vec![asset_id_yes.clone(), asset_id_no.clone()];
    let market_id = pm_market.condition_id.unwrap().clone();
    let pm_markets = vec![market_id.clone()];
    
    let mut pm_stream = pm_ws
        .subscribe_feed(pm_assets.clone(), pm_markets.clone())?
        .boxed();
    
    info!("Subscribed to Polymarket WebSocket feed");

    // -------------------------------------------------------------------------
    // 3. Kalshi Setup
    // -------------------------------------------------------------------------
    info!("Fetching Kalshi details...");
    let kalshi_event = kalshi
        .get_current_15min_market_by_symbol(symbol.clone())
        .await?;

    let kalshi_market_info = kalshi_event
        .markets
        .first()
        .ok_or_else(|| anyhow::anyhow!("No markets found in Kalshi event"))?;

    let kalshi_ticker = kalshi_market_info.ticker.clone();
    info!("Kalshi Found: {}", kalshi_ticker);

    // Subscribe to Kalshi
    kalshi.subscribe_market_ticker(kalshi_ticker.clone()).await?;
    info!("Subscribed to Kalshi WebSocket feed");

    // -------------------------------------------------------------------------
    // 4. Initialize Strategy Engine
    // -------------------------------------------------------------------------
    let mut market = DualExchangeMarket::new(
        asset_id_yes.clone(),
        asset_id_no.clone(),
        kalshi_ticker.clone()
    );

    // -------------------------------------------------------------------------
    // 5. Monitor Loop
    // -------------------------------------------------------------------------
    info!("---------------------------------------------------");
    info!("Listening for updates... (Press Ctrl+C to stop)");
    info!("---------------------------------------------------");

    let edge = Decimal::from_str(&edge.to_string()).unwrap();
    loop {
        // Reset opportunity flag for this loop iteration
        let mut opportunity_found = None;

        tokio::select! {
            // --- Handle Polymarket Messages ---
            Some(msg_result) = pm_stream.next() => {
                match msg_result {
                    Ok(update) => {
                        match update {
                            // UPDATED: Handle BestBidAsk instead of Orderbook
                            service::polymarket::ws::PolymarketFeedUpdate::BestBidAsk(bba) => {
                                // Update Strategy Engine
                                market.update_poly(bba);
                            },
                            service::polymarket::ws::PolymarketFeedUpdate::Trade(tr) => {
                                info!("[Polymarket] Trade: {} @ {}", tr.size, tr.price);
                            }
                        }
                        // Check for arb after update
                        opportunity_found = market.find_opportunity(edge.clone());
                    },
                    Err(e) => error!("[Polymarket] Stream Error: {:?}", e),
                }
            }

            // --- Handle Kalshi Messages (Unchanged) ---
            msg_result = kalshi.ws_client.next_message() => {
                match msg_result {
                    Ok(msg) => {
                        match msg {
                            // 2. Delta (Live Updates)
                            KalshiSocketMessage::TickerUpdate(ticker_update) => {
                                market.update_kalshi_delta(ticker_update.msg);
                                opportunity_found = market.find_opportunity(edge.clone());
                            },
                            
                            // 3. Ignored messages
                            _ => {}
                        }
                    },
                    Err(e) => {
                        error!("[Kalshi] Read Error: {:?}", e);
                        break; 
                    }
                }
            }
        }

        // --- Log Opportunity if Found ---
        if let Some(opp) = opportunity_found {
            info!("💰 ARBITRAGE FOUND 💰");
            info!("Strategy: {}", opp.description);
            info!("Total Cost: {}", opp.cost);
            info!("Potential Profit: {}", opp.potential_profit);
            info!("---------------------------------------------------");
        }
    }

    Ok(())
}