// src/monitor.rs
use std::str::FromStr;
use std::time::Duration;

use futures::StreamExt;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use lib::AppResult;
use polymarket_client_sdk::{clob::types::Side, types::U256};
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use service::{
    config::service::ConfigService,
    kalshi::service::KalshiService,
    market::Min15Symbols,
    polymarket::{
        clob::PolymarketClobService,
        fast_execution::PolyFastExecution,
        gamma::PolymarketGammaService,
        ws::{PolymarketFeedUpdate, PolymarketWebSocketService},
    },
    services::ServiceProvider,
    strategy::{
        handler::PositionHandler, market::{DualExchangeMarket, ExitOpportunity, Opportunity}, ArbSizing, FillEvent, Order, PairedPosition, Position
    },
};
use tracing::{error, info, warn};

#[derive(Debug, PartialEq, Clone)]
enum BotState {
    Scanning,
    Executing,
}

pub struct RunMonitorParams {
    pub services: ServiceProvider,
    pub min_entry_edge_cents: f64,
}

pub async fn run_monitor(params: RunMonitorParams) -> AppResult<()> {
    let services = params.services;
    // 1. Setup Services & Config
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
    let config_service = services.get_service_unchecked::<ConfigService>().await;

    let symbol = Min15Symbols::BTC;
    let edge_decimal = Decimal::from_str(&params.min_entry_edge_cents.to_string()).unwrap();

    // 2. Setup Market Data
    let pm_market = pm_gamma
        .get_current_15min_market_by_symbol(symbol.clone())
        .await?;
    let pm_tokens = pm_market
        .clob_token_ids
        .ok_or_else(|| anyhow::anyhow!("No CLOB tokens"))?;
    let (asset_yes, asset_no) = (pm_tokens[0], pm_tokens[1]);

    let mut pm_stream = pm_ws
        .subscribe_feed(
            vec![asset_yes, asset_no],
            vec![pm_market.condition_id.unwrap()],
        )?
        .boxed();

    let kalshi_event = kalshi
        .get_current_15min_market_by_symbol(symbol.clone())
        .await?;
    let kalshi_ticker = kalshi_event.markets[0].ticker.clone();
    kalshi
        .subscribe_market_ticker(kalshi_ticker.clone())
        .await?;
    kalshi.subscribe_fills().await?;

    // 3. Setup Strategy Components
    let mut market = DualExchangeMarket::new(asset_yes, asset_no, kalshi_ticker.clone());
    let mut handler = PositionHandler::new();

    // Execution Helpers
    let poly_fast_yes = PolyFastExecution::new(&pm_clob, config_service.clone(), asset_yes).await?;
    let poly_fast_no = PolyFastExecution::new(&pm_clob, config_service.clone(), asset_no).await?;

    let mut state = BotState::Scanning;
    let mut last_action = std::time::Instant::now();
    let cooldown = Duration::from_secs(5);

    info!(
        "🚀 Monitor Started: Poly[{}|{}] vs Kalshi[{}]",
        asset_yes, asset_no, kalshi_ticker
    );

    loop {
        tokio::select! {
            // --- Polymarket Stream ---
            Some(msg) = pm_stream.next() => {
                match msg {
                    Ok(PolymarketFeedUpdate::BestBidAsk(bba)) => market.update_poly(bba),
                    Ok(PolymarketFeedUpdate::Trade(trade)) => handler.handle_fill(FillEvent::Poly(trade)),
                    Err(e) => error!("Poly Stream Error: {:?}", e),
                }
            }

            // --- Kalshi Stream ---
            msg = kalshi.ws_client.next_message() => {
                match msg {
                    Ok(KalshiSocketMessage::TickerUpdate(t)) => market.update_kalshi_delta(t.msg),
                    Ok(KalshiSocketMessage::UserFill(fill)) => handler.handle_fill(FillEvent::Kalshi(fill.msg)),
                    Ok(_) => {}, // Ignore others
                    Err(e) => { error!("Kalshi WS Error: {:?}", e); break; }
                }
            }
        }

        // --- Bot Logic ---
        match state {
            BotState::Scanning => {
                if let Some(opp) = market.find_opportunity(edge_decimal) {
                    if last_action.elapsed() < cooldown {
                        continue;
                    }

                    info!("⚡ Opportunity: {}", opp.description);
                    state = BotState::Executing;

                    // Execute Entry
                    if let Some(pos) =
                        execute_entry(&opp, &poly_fast_yes, &poly_fast_no, &pm_clob, &kalshi).await
                    {
                        handler.set_position(pos);
                    } else {
                        state = BotState::Scanning; // Failed
                    }
                    last_action = std::time::Instant::now();
                }
            }
            _ => {} // Transients
        }
    }

    Ok(())
}

// --- Helper Functions to keep loop clean ---

async fn execute_entry(
    opp: &Opportunity,
    poly_yes: &PolyFastExecution,
    poly_no: &PolyFastExecution,
    pm_clob: &PolymarketClobService,
    kalshi: &KalshiService,
) -> Option<PairedPosition> { 
    let poly_cents = (opp.poly_price * Decimal::from(100)).ceil().to_u32().unwrap_or(100);
    let sizing = ArbSizing::calculate(Decimal::from(10), poly_cents, opp.kalshi_price_cents)?;
    let quantity_dec = Decimal::from(sizing.quantity);

    // 2. Prepare Poly
    let exec = if opp.poly_token_id == poly_yes.token_id { poly_yes } else { poly_no };
    
    // For BUY: makerAmount = USDC, takerAmount = Outcome Tokens.
    // We send 'quantity_dec' as size.
    let order = exec.build_fok_order(opp.poly_price, quantity_dec, Side::Buy).ok()?;

    // 3. Prepare Kalshi
    let k_order = Order::new(opp.kalshi_price_cents, sizing.quantity);

    info!("🚀 Executing Entry...");

    // 4. Fire
    let (p_res, k_res) = tokio::join!(
        exec.execute_order(pm_clob, order),
        kalshi.place_fok_order(
            opp.kalshi_ticker.clone(),
            opp.kalshi_side.clone(),
            "buy".to_string(),
            k_order
        )
    );

    match (p_res, k_res) {
        (Ok(poly_resp), Ok(_)) => {
            info!("✅ Entry Successful on both markets");
            
            // --- CRITICAL FIX: Use actual filled amount from Poly response ---
            // For a Buy order, taking_amount is the Outcome Tokens we received.
            let poly_filled_qty = poly_resp.taking_amount;
            
            info!("📝 Poly Filled Qty: {} (Requested: {})", poly_filled_qty, quantity_dec);

            let order_id = poly_resp.order_id.clone();
            let order = match pm_clob.get_order(&order_id).await {
                Ok(order) => order,
                Err(err) => {
                    error!("Failed to get order: {}", err);
                    return None
                }
            };
            
            info!("Open order response: {:?}", order);
            let size = order.size_matched;
            
            let size = size * Decimal::from_str("0.982").unwrap();
            
            info!("Poly response: {:?}", poly_resp);
            let poly_pos = Position {
                exchange: service::market::Exchange::Polymarket,
                id: opp.poly_token_id.to_string(), // Store ID as string
                side: "buy".to_string(), // We bought, so we hold it (to sell later)
                quantity: size, // Exact amount from response
                price: opp.poly_price,
                timestamp: std::time::Instant::now(),
            };

            let kalshi_pos = Position {
                exchange: service::market::Exchange::Kalshi,
                id: opp.kalshi_ticker.clone(),
                side: opp.kalshi_side.clone(),
                quantity: quantity_dec, // Kalshi currently assumed exact
                price: Decimal::from(opp.kalshi_price_cents) / Decimal::from(100),
                timestamp: std::time::Instant::now(),
            };

            Some(PairedPosition::new(poly_pos, kalshi_pos))
        }
        (Err(e), _) => { error!("❌ Poly Entry Failed: {:?}", e); None },
        (_, Err(e)) => { error!("❌ Kalshi Entry Failed: {:?}", e); None },
    }
}