use anyhow::bail;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{info, warn};
use polymarket_client_sdk::clob::types::Side;

// Updated imports
use crate::{market::exchange::Exchange, strategy::{FillEvent, PairedPosition}};

pub struct PositionHandler {
    pub current_position: PairedPosition, 
}


// implement position handler here