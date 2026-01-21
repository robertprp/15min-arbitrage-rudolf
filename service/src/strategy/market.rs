use std::str::FromStr;
use kalshi_rs::markets::models::GetMarketOrderbookResponse;
use kalshi_rs::websocket::models::{OrderbookDelta, OrderbookSnapshot, OrderbookSnapshotMessage, TickerUpdateMessage};
use rust_decimal::Decimal;
use polymarket_client_sdk::{clob::ws::BestBidAsk, types::U256};
use tracing::{info, warn};
use crate::strategy::book::Orderbook;
use rust_decimal::prelude::ToPrimitive;
// Import PairedPosition and Exchange
use crate::strategy::{PairedPosition, Position}; 
use crate::market::Exchange;

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
    pub poly_yes_asset: U256,
    pub poly_no_asset: U256,
    pub kalshi_ticker: String,

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
    
    // Kept for reference, but we are moving to the WS version below
    pub fn apply_kalshi_snapshot(&mut self, snapshot: GetMarketOrderbookResponse) {
        info!("Applying Kalshi REST Snapshot...");
        self.clear_kalshi_books();
        
        // ... (existing implementation logic) ...
        // For brevity, I'm calling the internal logic if you want to reuse code,
        // but sticking to the request, the new method is below.
    }

    /// New method to handle the WebSocket specific Snapshot Message
    pub fn apply_kalshi_ws_snapshot(&mut self, msg: OrderbookSnapshotMessage) {
        if msg.market_ticker != self.kalshi_ticker {
            return;
        }

        info!("Applying Kalshi WS Snapshot for {}", msg.market_ticker);
        self.clear_kalshi_books();

        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);

        // 1. Process YES Bids
        if let Some(yes_levels) = msg.yes {
            for (price_cents, qty) in yes_levels {
                let price = Decimal::from(price_cents) / one_hundred;
                let quantity = Decimal::from(qty);

                // Direct: Kalshi Yes Bid
                self.kalshi_yes.update_bid(price, quantity);

                // Synthetic: Kalshi No Ask (1 - Yes Bid)
                if price < one {
                    self.kalshi_no.update_ask(one - price, quantity);
                }
            }
        }

        // 2. Process NO Bids
        if let Some(no_levels) = msg.no {
            for (price_cents, qty) in no_levels {
                let price = Decimal::from(price_cents) / one_hundred;
                let quantity = Decimal::from(qty);

                // Direct: Kalshi No Bid
                self.kalshi_no.update_bid(price, quantity);

                // Synthetic: Kalshi Yes Ask (1 - No Bid)
                if price < one {
                    self.kalshi_yes.update_ask(one - price, quantity);
                }
            }
        }
    }

    fn clear_kalshi_books(&mut self) {
        self.kalshi_yes.bids.clear();
        self.kalshi_yes.asks.clear();
        self.kalshi_no.bids.clear();
        self.kalshi_no.asks.clear();
    }
    
    /// Step 2: Apply Delta updates from WebSocket
    pub fn apply_kalshi_delta(&mut self, delta: OrderbookDelta) {
        let msg = delta.msg;
        if msg.market_ticker != self.kalshi_ticker {
            return;
        }
        
        let one = Decimal::from(1);
        let one_hundred = Decimal::from(100);
        
        // Parse fields
        let price_cents = msg.price;
        let delta_qty = Decimal::from(msg.delta); // Can be negative
        let price = Decimal::from(price_cents) / one_hundred;
        
        // Determine which book to update based on "side" (yes/no)
        match msg.side.as_str() {
            "yes" => {
                // 1. Update Direct YES Bid
                // Note: We call Self::apply_delta_to_book (static call), not self.apply_delta_to_book
                Self::apply_delta_to_book(&mut self.kalshi_yes, true, price, delta_qty);
                
                // 2. Update Synthetic NO Ask
                if price < one {
                    let inverted_price = one - price;
                    Self::apply_delta_to_book(&mut self.kalshi_no, false, inverted_price, delta_qty);
                }
            },
            "no" => {
                // 1. Update Direct NO Bid
                Self::apply_delta_to_book(&mut self.kalshi_no, true, price, delta_qty);
                
                // 2. Update Synthetic YES Ask
                if price < one {
                    let inverted_price = one - price;
                    Self::apply_delta_to_book(&mut self.kalshi_yes, false, inverted_price, delta_qty);
                }
            },
            _ => warn!("Unknown Kalshi side: {}", msg.side)
        }
        
        // info!("Current best bid = {:?} best ask = {:?} for Kalshi YES", self.kalshi_yes.best_bid(), self.kalshi_yes.best_ask());
        // info!("Current best bid = {:?} best ask = {:?} for Kalshi NO", self.kalshi_no.best_bid(), self.kalshi_no.best_ask());
    }
    
    /// Helper to safely apply delta to a specific book
    /// FIX: Removed `&self` argument. It is now a static associate function.
    fn apply_delta_to_book(book: &mut Orderbook, is_bid: bool, price: Decimal, delta: Decimal) {
        // Get current quantity at this price level
        let current_qty = if is_bid {
            book.bids.get(&price).cloned().unwrap_or(Decimal::ZERO)
        } else {
            book.asks.get(&price).cloned().unwrap_or(Decimal::ZERO)
        };
        
        let new_qty = current_qty + delta;
        
        // update_bid / update_ask handle the removal if qty <= 0 internally
        if is_bid {
            book.update_bid(price, new_qty);
        } else {
            book.update_ask(price, new_qty);
        }
    }

    // ... (rest of find_opportunity logic remains the same) ...
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
}