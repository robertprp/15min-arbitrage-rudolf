// src/monitor.rs
use std::str::FromStr;
use std::time::Duration;

use futures::StreamExt;
use kalshi_rs::websocket::models::KalshiSocketMessage;
use lib::AppResult;
use polymarket_client_sdk::clob::types::Side;
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
        ArbSizing, FillEvent, Order, Position,
        handler::PositionHandler,
        market::{DualExchangeMarket, ExitOpportunity, Opportunity},
    },
};
use tracing::{error, info, warn};

#[derive(Debug, PartialEq, Clone)]
enum BotState {
    Scanning,
    Executing,
    Holding,
    Unwinding,
}

pub struct RunMonitorParams {
    pub services: ServiceProvider,
    pub min_entry_edge_cents: f64,
    pub min_unwind_edge_cents: f64,
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
    let unwind_min_profit_cents =
        Decimal::from_str(&params.min_unwind_edge_cents.to_string()).unwrap();
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
                        state = BotState::Holding;
                    } else {
                        state = BotState::Scanning; // Failed
                    }
                    last_action = std::time::Instant::now();
                }
            }
            BotState::Holding => {
                if let Some(pos) = &handler.current_position {
                    if let Some(exit) = market.find_exit_opportunity(pos, unwind_min_profit_cents) {
                        info!("🏁 Exit Signal: {}", exit.description);
                        state = BotState::Unwinding;

                        // Execute Exit
                        if execute_exit(
                            &pos,
                            &exit,
                            &poly_fast_yes,
                            &poly_fast_no,
                            &pm_clob,
                            &kalshi,
                        )
                        .await
                        {
                            handler.clear_position();
                            state = BotState::Scanning;
                        } else {
                            state = BotState::Holding; // Retry
                        }
                        last_action = std::time::Instant::now();
                    }
                } else {
                    // Handler lost position (maybe managed manually?), reset
                    state = BotState::Scanning;
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
) -> Option<Position> {
    // 1. Sizing
    let poly_cents = (opp.poly_price * Decimal::from(100))
        .ceil()
        .to_u32()
        .unwrap_or(100);
    let sizing = ArbSizing::calculate(Decimal::from(10), poly_cents, opp.kalshi_price_cents)?;

    info!("Executing Entry Size: {}", sizing.quantity);
    
    info!("Opportunity: {:?}", opp);
    info!("Sizing: {:?}", sizing);
    info!("Poly cents: {}", poly_cents);

    // 2. Prepare Poly
    let exec = if opp.poly_token_id == poly_yes.token_id {
        poly_yes
    } else {
        poly_no
    };
    let order = exec
        .build_fok_order(opp.poly_price, Decimal::from(sizing.quantity), Side::Buy)
        .ok()?;

    // 3. Prepare Kalshi
    let k_order = Order::new(opp.kalshi_price_cents, sizing.quantity);
    
    info!("Poly {:?}", order);
    info!("Kalshi {:?}", k_order);

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

    if p_res.is_ok() && k_res.is_ok() {
        info!("✅ Entry Successful on both markets");
        Some(Position {
            quantity: sizing.quantity,
            entry_cost_per_unit: opp.cost,
            entry_time: std::time::Instant::now(),
            poly_token_id: opp.poly_token_id,
            poly_side: "buy".to_string(),
            kalshi_ticker: opp.kalshi_ticker.clone(),
            kalshi_side: opp.kalshi_side.clone(),
        })
    } else {
        error!(
            "❌ Entry Failed: Poly {:?}, Kalshi {:?}",
            p_res.err(),
            k_res.err()
        );
        None
    }
}

async fn execute_exit(
    pos: &Position,
    exit: &ExitOpportunity,
    poly_yes: &PolyFastExecution,
    poly_no: &PolyFastExecution,
    pm_clob: &PolymarketClobService,
    kalshi: &KalshiService,
) -> bool {
    // 1. Prepare Execution Context
    let exec = if pos.poly_token_id == poly_yes.token_id {
        poly_yes
    } else {
        poly_no
    };
    
    info!("Position {:?}", pos);
    info!("Exit {:?}", exit);
    
    // Calculate Kalshi parameters once (they don't change on retry)
    let k_cents = (exit.kalshi_bid_price * Decimal::from(100))
        .to_u32()
        .unwrap_or(1);
    
    info!("Kalshi cents: {}", k_cents);
    let k_order = Order::new(k_cents, pos.quantity);

    info!("Kalshi order: {:?}", k_order);
    // Track status
    let mut poly_done = false;
    let mut kalshi_done = false;

    // Build initial Poly order
    let poly_order_attempt = exec.build_fok_order(
        exit.poly_bid_price, 
        Decimal::from(pos.quantity), 
        Side::Sell
    );

    // If we can't even build the first order, fail immediately before sending anything
    let poly_order = match poly_order_attempt {
        Ok(o) => o,
        Err(e) => {
            error!("❌ Failed to build initial Poly exit order: {:?}", e);
            return false;
        }
    };
    info!("Poly order: {:?}", poly_order);

    info!("📉 Attempting Exit: Poly Price {}, Kalshi Cents {}", exit.poly_bid_price, k_cents);

    let (p_res, k_res) = tokio::join!(
        exec.execute_order(pm_clob, poly_order),
        kalshi.place_fok_order(
            pos.kalshi_ticker.clone(),
            pos.kalshi_side.clone(),
            "sell".to_string(),
            k_order.clone()
        )
    );

    if p_res.is_ok() { poly_done = true; }
    if k_res.is_ok() { kalshi_done = true; }

    // --- Retry Logic for Partial Fills ---

    let max_retries = 5;
    let mut attempts = 0;

    while (!poly_done || !kalshi_done) && attempts < max_retries {
        // If both failed initially, we might return false immediately, 
        // OR we can try to retry both. Here we retry both if both failed, 
        // but usually, if both fail, it's a signal issue. 
        // We prioritize the case where ONE succeeded and we are "legged".
        
        attempts += 1;
        warn!(
            "⚠️ Partial Exit Detected! Poly: {}, Kalshi: {}. Retrying attempt {}/{}", 
            if poly_done { "✅" } else { "❌" },
            if kalshi_done { "✅" } else { "❌" },
            attempts, max_retries
        );

        // Retry Polymarket
        if !poly_done {
            // Must REBUILD order to get a fresh Salt/Timestamp
            if let Ok(new_order) = exec.build_fok_order(exit.poly_bid_price, Decimal::from(pos.quantity), Side::Sell) {
                match exec.execute_order(pm_clob, new_order).await {
                    Ok(_) => {
                        info!("✅ Polymarket Exit Retry Successful");
                        poly_done = true;
                    },
                    Err(e) => error!("❌ Poly Retry Failed: {:?}", e),
                }
            }
        }

        // Retry Kalshi
        if !kalshi_done {
            // Kalshi API handles new orders fine, just resend
            match kalshi.place_fok_order(
                pos.kalshi_ticker.clone(), 
                pos.kalshi_side.clone(), 
                "sell".to_string(), 
                k_order.clone()
            ).await {
                Ok(_) => {
                    info!("✅ Kalshi Exit Retry Successful");
                    kalshi_done = true;
                },
                Err(e) => error!("❌ Kalshi Retry Failed: {:?}", e),
            }
        }
    }

    // --- Final Result ---

    if poly_done && kalshi_done {
        info!("🏁 Exit Fully Complete. Profit: {}", exit.profit);
        true
    } else {
        error!(
            "💀 Critical Exit Failure after retries. Poly Filled: {}, Kalshi Filled: {}. Manual Intervention Required.", 
            poly_done, kalshi_done
        );
        // Even if partially filled, we return false so the bot state doesn't reset to Scanning 
        // cleanly without the operator knowing something is wrong. 
        // (Or you might want to return true to stop the bot from trying to sell the *whole* amount again).
        // For safety here: false.
        false
    }
}