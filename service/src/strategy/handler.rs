use anyhow::bail;
use polymarket_client_sdk::clob::types::Side;
use rust_decimal::Decimal;
use rust_decimal::prelude::ToPrimitive;
use tracing::{info, warn};

// Updated imports
use crate::{
    market::exchange::Exchange,
    strategy::{FillEvent, PairedPosition},
};

