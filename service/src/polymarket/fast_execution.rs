use std::str::FromStr;
use std::sync::Arc;
use alloy::primitives::{Address, U256};
use alloy::signers::{Signer};
use alloy::signers::k256::ecdsa::SigningKey;
use anyhow::{bail, Context};
use polymarket_client_sdk::auth::LocalSigner;
use polymarket_client_sdk::clob::types::{Order, OrderType, SignableOrder, Side};
use polymarket_client_sdk::types::Decimal;
use rust_decimal::prelude::ToPrimitive;
use rand::Rng;
use std::time::{SystemTime, UNIX_EPOCH};

use crate::polymarket::clob::PolymarketClobService;
use crate::config::service::ConfigService;
use lib::AppResult;

// Constants from SDK
const USDC_DECIMALS: u32 = 6;
const LOT_SIZE_SCALE: u32 = 2;

/// Caches static market data to avoid HTTP calls during execution
pub struct PolyFastExecution {
    pub signer: LocalSigner<SigningKey>,
    pub funder: Address,
    pub chain_id: u64,
    // Cache specific to the pair we are trading
    pub token_id: U256,
    pub fee_rate_bps: U256,
    pub tick_size_decimal: Decimal,
}

impl PolyFastExecution {
    pub async fn new(
        clob_service: &PolymarketClobService, 
        config: Arc<ConfigService>,
        token_id: U256
    ) -> AppResult<Self> {
        // 1. Fetch static data ONCE via the slow SDK method
        let fee_rate = clob_service.client.fee_rate_bps(token_id).await?;
        let tick_data = clob_service.client.tick_size(token_id).await?;
        
        let signer = clob_service.signer.clone(); 
        let funder = Address::from_str(&config.polymarket.funder)?;

        Ok(Self {
            signer,
            funder,
            chain_id: config.chain.chain_id,
            token_id,
            fee_rate_bps: U256::from(fee_rate.base_fee),
            tick_size_decimal: tick_data.minimum_tick_size.as_decimal(),
        })
    }

    /// Pure math construction of the order (0 network calls)
    pub fn build_fok_order(
        &self,
        price: Decimal,
        size: Decimal,
        side: Side,
    ) -> AppResult<SignableOrder> {
        let decimals = self.tick_size_decimal.scale();

        // 1. Calculate Maker/Taker amounts (Logic copied from SDK)
        let (taker_amount, maker_amount) = match side {
            Side::Buy => (
                size,
                (size * price).trunc_with_scale(decimals + LOT_SIZE_SCALE),
            ),
            Side::Sell => (
                (size * price).trunc_with_scale(decimals + LOT_SIZE_SCALE),
                size,
            ),
            _ => {
                bail!("Invalid side")
            }
        };

        // 2. Generate Salt (Local math)
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let seconds = now.as_secs_f64();
        let rand = rand::random::<f64>();
        let raw_salt = (seconds * rand).round() as u64;
        let salt = raw_salt & ((1 << 53) - 1); // IEEE 754 Safe Integer

        let mut order = Order::default();
        order.salt = U256::from(salt);
        order.maker = self.funder;
        order.taker = Address::ZERO;
        order.tokenId = self.token_id;
        order.makerAmount = U256::from(Self::to_fixed_u128(maker_amount));
        order.takerAmount = U256::from(Self::to_fixed_u128(taker_amount));
        order.side = side as u8;
        order.feeRateBps = self.fee_rate_bps;
        order.nonce = U256::from(0); // FOK/GTC usually doesn't strictly require sequential nonce management if salt is unique
        order.signer = self.signer.address();
        order.expiration = U256::ZERO; // 0 for GTC/FOK

        let signable_order = SignableOrder::builder()
            .order(order)
            .order_type(OrderType::GTC)
            .post_only(false)
            .build();

        Ok(signable_order)
    }
    
    // Helper to match SDK math
    fn to_fixed_u128(d: Decimal) -> u128 {
        d.normalize()
            .trunc_with_scale(USDC_DECIMALS)
            .mantissa()
            .to_u128()
            .unwrap_or(0)
    }

    /// Sign and Post (Network Call 1)
    pub async fn execute_order(&self, clob_service: &PolymarketClobService, signable: SignableOrder) -> AppResult<String> {
        // Sign
        let signed_order = clob_service.client.sign(&self.signer, signable).await?;
        
        // Post
        let response = clob_service.client.post_order(signed_order).await?;
        
        if response.success {
            Ok(response.order_id)
        } else {
             anyhow::bail!("Order failed: {:?}", response.error_msg);
        }
    }
}