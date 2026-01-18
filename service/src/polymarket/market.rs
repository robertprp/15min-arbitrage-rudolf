use alloy::primitives::U256;
use anyhow::bail;
use lib::AppResult;
use polymarket_client_sdk::gamma::types::response::Market;

pub struct PolymarketMarket {
    market: Market,
}

impl PolymarketMarket {
    pub fn new(market: Market) -> Self {
        Self { market }
    }
    
    pub fn clob_token_ids(&self) -> AppResult<[U256; 2]> {
        match self.market.clob_token_ids.clone() {
            Some(tokens) => Ok([tokens[0].clone(), tokens[1].clone()]),
            None => bail!("Market does not have clob token ids"),
        }
    }
    
    pub fn get_clob_yes_token_id(&self) -> AppResult<U256> {
        match self.clob_token_ids() {
            Ok(ids) => Ok(ids[0]),
            Err(e) => Err(e),
        }
    }
    
    pub fn get_clob_no_token_id(&self) -> AppResult<U256> {
        match self.clob_token_ids() {
            Ok(ids) => Ok(ids[1]),
            Err(e) => Err(e),
        }
    }
}
