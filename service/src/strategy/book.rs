use std::collections::BTreeMap;
use rust_decimal::Decimal;

#[derive(Debug, Clone, Default)]
pub struct Orderbook {
    // Bids: Sorted by Price. Last element is highest price (Best Bid).
    pub bids: BTreeMap<Decimal, Decimal>, 
    // Asks: Sorted by Price. First element is lowest price (Best Ask).
    pub asks: BTreeMap<Decimal, Decimal>, 
}

impl Orderbook {
    pub fn new() -> Self {
        Self::default()
    }

    pub fn update_bid(&mut self, price: Decimal, size: Decimal) {
        if size <= Decimal::ZERO {
            self.bids.remove(&price);
        } else {
            self.bids.insert(price, size);
        }
    }

    pub fn update_ask(&mut self, price: Decimal, size: Decimal) {
        if size <= Decimal::ZERO {
            self.asks.remove(&price);
        } else {
            self.asks.insert(price, size);
        }
    }

    /// Best price to SELL into (Highest Bid)
    pub fn best_bid(&self) -> Option<Decimal> {
        self.bids.keys().next_back().cloned()
    }

    /// Best price to BUY from (Lowest Ask)
    pub fn best_ask(&self) -> Option<Decimal> {
        self.asks.keys().next().cloned()
    }
}