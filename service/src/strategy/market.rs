use std::str::FromStr;
use kalshi_rs::websocket::models::TickerUpdateMessage;
use rust_decimal::Decimal;
use polymarket_client_sdk::{clob::ws::BestBidAsk, types::U256};
use crate::strategy::book::Orderbook;
use rust_decimal::prelude::ToPrimitive;

// Assuming TickerUpdateMessage is defined in another module or locally
// use kalshi_rs::websocket::models::TickerUpdateMessage; 

#[derive(Debug)]
pub struct Opportunity {
    pub description: String,
    pub cost: Decimal,
    pub potential_profit: Decimal,
    
    // 
    pub poly_token_id: U256,
    pub poly_price: Decimal,
    pub kalshi_ticker: String,
    pub kalshi_side: String, // "yes" or "no"
    pub kalshi_price_cents: u32,
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

    // --- Polymarket Update ---
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

    // --- Kalshi Update (FIXED) ---
    pub fn update_kalshi_delta(&mut self, ticker_update: TickerUpdateMessage) {
        // Optional: Ensure we are updating the correct ticker
        if ticker_update.market_ticker != self.kalshi_ticker {
            return;
        }
        
        // Clear old snapshots
        self.kalshi_yes.bids.clear();
        self.kalshi_yes.asks.clear();
        self.kalshi_no.bids.clear();
        self.kalshi_no.asks.clear();

        let one_hundred = Decimal::from(100);
        let one = Decimal::from(1);

        // 1. Process Yes Bid
        // If there is a bid for Yes, we add it to kalshi_yes.bids
        // AND we calculate the implied Ask for No (1 - YesBid)
        if ticker_update.yes_bid > 0 {
            let yes_bid_price = Decimal::from(ticker_update.yes_bid) / one_hundred;
            
            // Explicit Yes Bid
            self.kalshi_yes.update_bid(yes_bid_price, one);

            // Implied No Ask: Buying No is mathematically selling Yes
            // Price = 1.00 - yes_bid_price
            let no_ask_price = one - yes_bid_price;
            self.kalshi_no.update_ask(no_ask_price, one);
        }

        // 2. Process Yes Ask
        // If there is an ask for Yes, we add it to kalshi_yes.asks
        // AND we calculate the implied Bid for No (1 - YesAsk)
        if ticker_update.yes_ask > 0 {
            let yes_ask_price = Decimal::from(ticker_update.yes_ask) / one_hundred;

            // Explicit Yes Ask
            self.kalshi_yes.update_ask(yes_ask_price, one);

            // Implied No Bid: Selling No is mathematically buying Yes
            // Price = 1.00 - yes_ask_price
            let no_bid_price = one - yes_ask_price;
            self.kalshi_no.update_bid(no_bid_price, one);
        }
    }
    
    // --- Arbitrage Logic ---
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
                    
                    // Calculate Kalshi price in cents: (k_ask * 100).ceil()
                    let k_cents = (k_ask * one_hundred).ceil().to_u32().unwrap_or(99);
    
                    return Some(Opportunity {
                        description: format!("Buy Poly YES ({}) + Buy Kalshi NO ({}). Edge: {}", p_ask, k_ask, edge),
                        cost: real_cost,
                        potential_profit: one - real_cost,
                        // Execution
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
                        description: format!("Buy Poly NO ({}) + Buy Kalshi YES ({}). Edge: {}", p_ask, k_ask, edge),
                        cost: real_cost,
                        potential_profit: one - real_cost,
                        // Execution
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
}