use std::str::FromStr;
use kalshi_rs::websocket::models::TickerUpdateMessage;
use rust_decimal::Decimal;
use polymarket_client_sdk::{clob::ws::BestBidAsk, types::U256};
use crate::strategy::book::Orderbook;
use rust_decimal::prelude::ToPrimitive;
use crate::strategy::Position;

#[derive(Debug)]
pub struct Opportunity {
    pub description: String,
    pub cost: Decimal,
    pub potential_profit: Decimal,
    
    // Execution details
    pub poly_token_id: U256,
    pub poly_price: Decimal,
    pub kalshi_ticker: String,
    pub kalshi_side: String, // "yes" or "no"
    pub kalshi_price_cents: u32,
}

#[derive(Debug)]
pub struct ExitOpportunity {
    pub description: String,
    pub exit_value: Decimal,
    pub profit: Decimal,
    
    // Execution details (Prices to Sell at)
    pub poly_bid_price: Decimal,
    pub kalshi_bid_price: Decimal, 
}

pub struct DualExchangeMarket {
    poly_yes_asset: U256,
    poly_no_asset: U256,
    kalshi_ticker: String,

    pub poly_yes: Orderbook,
    pub poly_no: Orderbook,
    pub kalshi_yes: Orderbook, 
    pub kalshi_no: Orderbook,  
}

impl DualExchangeMarket {
    pub fn new(poly_yes: U256, poly_no: U256, kalshi_ticker: String) -> Self {
        Self {
            poly_yes_asset: poly_yes,
            poly_no_asset: poly_no,
            kalshi_ticker,
            poly_yes: Orderbook::new(),
            poly_no: Orderbook::new(),
            kalshi_yes: Orderbook::new(), 
            kalshi_no: Orderbook::new(),
        }
    }

    // ... [update_poly and update_kalshi_delta remain unchanged] ...
    pub fn update_poly(&mut self, update: BestBidAsk) {
        let book = if update.asset_id == self.poly_yes_asset {
            &mut self.poly_yes
        } else if update.asset_id == self.poly_no_asset {
            &mut self.poly_no
        } else {
            return;
        };

        book.bids.clear();
        book.asks.clear();

        if !update.best_bid.is_zero() {
            book.update_bid(update.best_bid, Decimal::from(1));
        }
        if !update.best_ask.is_zero() {
            book.update_ask(update.best_ask, Decimal::from(1));
        }
    }

    pub fn update_kalshi_delta(&mut self, ticker_update: TickerUpdateMessage) {
        if ticker_update.market_ticker != self.kalshi_ticker {
            return;
        }
        
        self.kalshi_yes.bids.clear();
        self.kalshi_yes.asks.clear();
        self.kalshi_no.bids.clear();
        self.kalshi_no.asks.clear();

        let one_hundred = Decimal::from(100);
        let one = Decimal::from(1);

        // Process Bids/Asks (Logic unchanged from original file)
        if ticker_update.yes_bid > 0 {
            let yes_bid_price = Decimal::from(ticker_update.yes_bid) / one_hundred;
            self.kalshi_yes.update_bid(yes_bid_price, one);
            let no_ask_price = one - yes_bid_price;
            self.kalshi_no.update_ask(no_ask_price, one);
        }

        if ticker_update.yes_ask > 0 {
            let yes_ask_price = Decimal::from(ticker_update.yes_ask) / one_hundred;
            self.kalshi_yes.update_ask(yes_ask_price, one);
            let no_bid_price = one - yes_ask_price;
            self.kalshi_no.update_bid(no_bid_price, one);
        }
    }
    
    // --- Entry Logic (Buying) ---
    pub fn find_opportunity(&self, edge: Decimal) -> Option<Opportunity> {
            let one = Decimal::from(1);
            let one_hundred = Decimal::from(100);
            
            // Strategy A: Buy Poly YES + Buy Kalshi NO
            let p_yes_ask = self.poly_yes.best_ask();
            let k_no_ask = self.kalshi_no.best_ask(); 
    
            if let (Some(p_ask), Some(k_ask)) = (p_yes_ask, k_no_ask) {
                let total_cost = p_ask + k_ask + edge;
                
                if total_cost < one {
                    let real_cost = p_ask + k_ask;
                    let k_cents = (k_ask * one_hundred).ceil().to_u32().unwrap_or(99);
    
                    return Some(Opportunity {
                        description: format!("Buy Poly YES ({}) + Buy Kalshi NO ({}). Cost: {}", p_ask, k_ask, real_cost),
                        cost: real_cost,
                        potential_profit: one - real_cost,
                        poly_token_id: self.poly_yes_asset,
                        poly_price: p_ask,
                        kalshi_ticker: self.kalshi_ticker.clone(),
                        kalshi_side: "no".to_string(),
                        kalshi_price_cents: k_cents
                    });
                }
            }
    
            // Strategy B: Buy Poly NO + Buy Kalshi YES
            let p_no_ask = self.poly_no.best_ask();
            let k_yes_ask = self.kalshi_yes.best_ask(); 
    
            if let (Some(p_ask), Some(k_ask)) = (p_no_ask, k_yes_ask) {
                 let total_cost = p_ask + k_ask + edge;
    
                 if total_cost < one {
                     let real_cost = p_ask + k_ask;
                     let k_cents = (k_ask * one_hundred).ceil().to_u32().unwrap_or(99);
    
                     return Some(Opportunity {
                        description: format!("Buy Poly NO ({}) + Buy Kalshi YES ({}). Cost: {}", p_ask, k_ask, real_cost),
                        cost: real_cost,
                        potential_profit: one - real_cost,
                        poly_token_id: self.poly_no_asset,
                        poly_price: p_ask,
                        kalshi_ticker: self.kalshi_ticker.clone(),
                        kalshi_side: "yes".to_string(),
                        kalshi_price_cents: k_cents
                    });
                }
            }
    
            None
    }

    // --- Exit Logic (Selling/Unwinding) ---
    pub fn find_exit_opportunity(&self, position: &Position, min_profit: Decimal) -> Option<ExitOpportunity> {
        // We need to Sell what we Hold.
        // We sell into the Bids.
        
        let p_bid_opt;
        let k_bid_opt;

        // 1. Identify which books to check based on position
        if position.poly_token_id == self.poly_yes_asset {
            // We hold Poly YES, we need to sell Poly YES
            p_bid_opt = self.poly_yes.best_bid();
        } else {
            // We hold Poly NO
            p_bid_opt = self.poly_no.best_bid();
        }

        if position.kalshi_side == "yes" {
            // We hold Kalshi YES, sell Kalshi YES
            k_bid_opt = self.kalshi_yes.best_bid();
        } else {
            // We hold Kalshi NO, sell Kalshi NO
            k_bid_opt = self.kalshi_no.best_bid();
        }

        // 2. Check if prices exist
        if let (Some(p_bid), Some(k_bid)) = (p_bid_opt, k_bid_opt) {
            let exit_value = p_bid + k_bid;
            let target_exit_value = position.entry_cost_per_unit * (Decimal::from(1) + min_profit);

            // 3. Check if exit value meets target
            if exit_value >= target_exit_value {
                let profit = exit_value - position.entry_cost_per_unit;
                return Some(ExitOpportunity {
                    description: format!("Unwind: Sell Poly ({}) + Sell Kalshi ({}). Total: {}", p_bid, k_bid, exit_value),
                    exit_value,
                    profit,
                    poly_bid_price: p_bid,
                    kalshi_bid_price: k_bid
                });
            }
        }

        None
    }
}