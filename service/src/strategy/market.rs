use polymarket_client_sdk::types::U256;
use rust_decimal::Decimal;
// Import PairedPosition and Exchange

#[derive(Debug)]
pub struct Opportunity {
    // Execution details
    pub poly_token_id: U256,
    pub poly_price: Decimal,
    pub kalshi_ticker: String,
    pub kalshi_side: String, // "yes" or "no"
    pub kalshi_price_cents: u32,

    pub timestamp: u64,
}
