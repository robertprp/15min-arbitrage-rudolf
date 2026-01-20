use std::str::FromStr;
use std::time::Duration;

use futures::StreamExt;
use rust_decimal::prelude::ToPrimitive;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use lib::AppResult;
use rust_decimal::{prelude::FromPrimitive, Decimal};
use service::{
    kalshi::service::KalshiService,
    market::Min15Symbols,
    polymarket::{clob::PolymarketClobService, gamma::PolymarketGammaService, ws::PolymarketWebSocketService},
    services::ServiceProvider, strategy::{market::DualExchangeMarket, ArbSizing, Order},
};
use tracing::{error, info, warn};

pub async fn run_monitor(services: ServiceProvider, edge: f64) -> AppResult<()> {
    // 1. Get Services
    let pm_gamma = services.get_service_unchecked::<PolymarketGammaService>().await;
    let pm_ws = services.get_service_unchecked::<PolymarketWebSocketService>().await;
    let pm_clob = services.get_service_unchecked::<PolymarketClobService>().await;
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

    info!("Polymarket Found: {} (YES: {}, NO: {})", pm_market.slug.unwrap_or_default(), asset_id_yes, asset_id_no);

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

    let edge_decimal = Decimal::from_str(&edge.to_string()).unwrap();
    
    // Simple cooldown to prevent spamming if logic is sticky
    let mut last_trade_time = std::time::Instant::now(); 
    let cooldown = Duration::from_secs(10);

    loop {
        let mut opportunity_found = None;

        tokio::select! {
            // --- Handle Polymarket Messages ---
            Some(msg_result) = pm_stream.next() => {
                match msg_result {
                    Ok(update) => {
                        match update {
                            service::polymarket::ws::PolymarketFeedUpdate::BestBidAsk(bba) => {
                                market.update_poly(bba);
                            },
                            service::polymarket::ws::PolymarketFeedUpdate::Trade(_) => {}
                        }
                        opportunity_found = market.find_opportunity(edge_decimal.clone());
                    },
                    Err(e) => error!("[Polymarket] Stream Error: {:?}", e),
                }
            }

            // --- Handle Kalshi Messages ---
            msg_result = kalshi.ws_client.next_message() => {
                match msg_result {
                    Ok(msg) => {
                        if let KalshiSocketMessage::TickerUpdate(ticker_update) = msg {
                            market.update_kalshi_delta(ticker_update.msg);
                            opportunity_found = market.find_opportunity(edge_decimal.clone());
                        }
                    },
                    Err(e) => {
                        error!("[Kalshi] Read Error: {:?}", e);
                        break; 
                    }
                }
            }
        }

        // --- Execute Arbitrage if Found ---
        if let Some(opp) = opportunity_found {
            // Check Cooldown
            if last_trade_time.elapsed() < cooldown {
                warn!("Opportunity ignored due to cooldown");
                continue;
            }
            let poly_price_cents = (opp.poly_price * Decimal::from(100))
                .ceil()
                .to_u32()
                .unwrap_or(100); // Fallback to 100 (max price) if conversion fails
            
            info!("Poly price: {}", poly_price_cents);
            info!("Kalshi price: {}", opp.kalshi_price_cents);
            
            // 2. Calculate Sizing
            let sizing_opt = ArbSizing::calculate(
                Decimal::from(10), 
                poly_price_cents,      // u32
                opp.kalshi_price_cents // u32
            );
            
            let sizing = match sizing_opt {
                Some(s) => s,
                None => {
                    warn!("Opportunity found but sizing failed (Price too high or Budget too low)");
                    continue;
                }
            };

            info!("🚀 ARBITRAGE DETECTED: Executing...");
            
            info!("🚀 ARBITRAGE DETECTED: Executing...");
            info!("Strategy: {}", opp.description);
            info!("Prices (Cents): Poly: {}¢, Kalshi: {}¢", poly_price_cents, opp.kalshi_price_cents);
            info!("Sizing: {} contracts. Total Cost: ${}", sizing.quantity, sizing.total_cost_usd);
            
            let k_order = Order::new(opp.kalshi_price_cents, sizing.quantity);
            // ...
            let p_size = Decimal::from(sizing.quantity);
            // 1. Prepare Kalshi Params
            let k_ticker = opp.kalshi_ticker.clone();
            let k_side = opp.kalshi_side.clone();
            
            let one_hundred = Decimal::from_f64(100.0).unwrap();
            
            let p_token = opp.poly_token_id;
            let p_price = opp.poly_price;
            
            info!("K_order {:?}", k_order);
            info!("K_ticker {:?}", k_ticker);
            info!("K_side {:?}", k_side);
            info!("K_order_usd {:?}", k_order.price_usd());
            
            // 3. EXECUTE CONCURRENTLY (Latency Critical)
            // We clone references to services to move them into the async block
            let kalshi_ref = kalshi.clone();
            let pm_clob_ref = pm_clob.clone();

            let poly_future = async move {
                pm_clob_ref.place_fok_order(p_token, p_price, p_size, 0).await
            };

            let kalshi_future = async move {
                kalshi_ref.place_fok_order(k_ticker, k_side, "buy".to_string(), k_order).await
            };

            // Fire both at once!
            let (poly_res, kalshi_res) = tokio::join!(poly_future, kalshi_future);

            // 4. Log Results
            match poly_res {
                Ok(id) => info!("✅ Poly Order Placed: {}", id),
                Err(e) => error!("❌ Poly Order Failed: {:?}", e),
            }

            match kalshi_res {
                Ok(_) => info!("✅ Kalshi Order Placed"),
                Err(e) => error!("❌ Kalshi Order Failed: {:?}", e),
            }
            
            info!("Strategy: {}", opp.description);
            info!("---------------------------------------------------");
            
            last_trade_time = std::time::Instant::now();
        }
    }

    Ok(())
}