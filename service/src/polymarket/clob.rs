use std::str::FromStr;
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

use alloy::primitives::{Address, U256};
use alloy::signers::{Signer, k256::ecdsa::SigningKey, local::LocalSigner};
use anyhow::{Context, bail};
use async_trait::async_trait;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::clob::types::response::{
    FeeRateResponse, OpenOrderResponse, PostOrderResponse, TickSizeResponse,
};
use polymarket_client_sdk::clob::types::{Order, OrderType, Side, SignableOrder, SignatureType};
use polymarket_client_sdk::{
    POLYGON,
    auth::state::Authenticated,
    clob::{Client, Config},
    types::Decimal,
};
 // Ensure 'rand' is in your Cargo.toml
use rust_decimal::prelude::ToPrimitive; // Ensure 'rust_decimal' is in your Cargo.toml
use tracing::info;

use crate::{
    config::service::ConfigService,
    services::{ServiceFactory, ServiceProvider},
};
use lib::AppResult;

// Constants moved from PolyFastExecution
const USDC_DECIMALS: u32 = 6;
const LOT_SIZE_SCALE: u32 = 2;

pub struct PolymarketClobService {
    // We hold the client in an Authenticated state with the LocalSigner generic
    pub client: Client<Authenticated<Normal>>,
    // We must hold the signer separately to pass it to the .sign() method
    pub signer: LocalSigner<SigningKey>,
}

impl PolymarketClobService {
    pub async fn new(config_service: Arc<ConfigService>) -> AppResult<Self> {
        let chain_config = &config_service.chain;

        // 1. Setup Signer (L2 Polygon Key)
        let private_key = &chain_config.private_key;
        let signer = LocalSigner::from_str(private_key)
            .context("Failed to parse Private Key for CLOB signer")?
            .with_chain_id(Some(POLYGON));

        // 2. Setup Client Config
        // Use server time to avoid timestamp sync issues
        let client_config = Config::builder().use_server_time(true).build();

        let funder = config_service.polymarket.funder.clone();

        let client = Client::new("https://clob.polymarket.com", client_config)?
            .authentication_builder(&signer)
            .signature_type(SignatureType::GnosisSafe)
            .funder(Address::from_str(&funder)?)
            .authenticate()
            .await
            .context("Failed to authenticate Polymarket CLOB client")?;

        info!("Polymarket CLOB Service authenticated successfully");

        Ok(Self { client, signer })
    }

    pub async fn get_order(&self, order_id: &str) -> AppResult<OpenOrderResponse> {
        let response = self
            .client
            .order(order_id)
            .await
            .context("Failed to get order")?;

        Ok(response)
    }

    pub async fn get_fee_rate_bps(&self, token_id: U256) -> AppResult<FeeRateResponse> {
        let response = self
            .client
            .fee_rate_bps(token_id)
            .await
            .context("Failed to get fee rate")?;

        Ok(response)
    }

    pub async fn get_tick_size(&self, token_id: U256) -> AppResult<TickSizeResponse> {
        let response = self
            .client
            .tick_size(token_id)
            .await
            .context("Failed to get tick size")?;

        Ok(response)
    }
}

#[async_trait]
impl ServiceFactory for PolymarketClobService {
    async fn factory(services: ServiceProvider) -> AppResult<Self> {
        let config_service = services.get_service_unchecked::<ConfigService>().await;
        Self::new(config_service).await
    }
}

pub struct PolymarketExecutionService {
    clob: Arc<PolymarketClobService>,

    // Cached/Static Data
    pub token_id: U256,
    pub funder: Address,
    pub chain_id: u64,
    pub fee_rate_bps: U256,
    pub tick_size_decimal: Decimal,
}

impl PolymarketExecutionService {
    pub async fn new(
        clob: Arc<PolymarketClobService>,
        config: Arc<ConfigService>,
        token_id: U256,
    ) -> AppResult<Self> {
        // 1. Fetch static data ONCE via the CLOB service
        let fee_rate = clob.get_fee_rate_bps(token_id).await?;
        let tick_data = clob.get_tick_size(token_id).await?;

        let funder = Address::from_str(&config.polymarket.funder)?;

        Ok(Self {
            clob,
            funder,
            chain_id: config.chain.chain_id,
            token_id,
            fee_rate_bps: U256::from(fee_rate.base_fee),
            tick_size_decimal: tick_data.minimum_tick_size.as_decimal(),
        })
    }

    /// Pure math construction of the order (0 network calls)
    pub fn build_limit_order(
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
        // FOK/GTC usually doesn't strictly require sequential nonce management if salt is unique
        order.nonce = U256::from(0);
        // Use the address from the CLOB service signer
        order.signer = self.clob.signer.address();
        order.expiration = U256::from(0);
        order.signatureType = SignatureType::GnosisSafe as u8;

        let signable_order = SignableOrder::builder()
            .order(order)
            .order_type(OrderType::GTC)
            .post_only(false)
            .build();

        Ok(signable_order)
    }

    pub fn build_market_order(
        &self,
        raw_price: Decimal,
        size: Decimal, // Assumed to be SHARES
        side: Side,
    ) -> AppResult<SignableOrder> {
        let decimals = self.tick_size_decimal.scale();
        let min_tick_size = self.tick_size_decimal;

        // 1. Validate and Truncate Price
        // Ensure that the market price is truncated to our tick size
        let price = raw_price.trunc_with_scale(decimals);

        if price < min_tick_size || price > Decimal::ONE - min_tick_size {
            anyhow::bail!(
                "Price {} is too small or too large for the minimum tick size {}",
                price,
                min_tick_size
            );
        }

        // 2. Calculate Maker/Taker amounts
        // Logic adapted to assume 'size' is always SHARES for FOK orders.
        // If you need to support Buying with USDC amount, a logic branch is needed.
        let (taker_amount, maker_amount) = match side {
            // Buy N shares: use cutoff `price` derived from ask depth
            // Taker takes SHARES, Maker gives USDC
            Side::Buy => {
                let usdc = (size * price).trunc_with_scale(decimals + LOT_SIZE_SCALE);
                (size, usdc)
            }

            // Sell N shares for USDC
            // Taker takes USDC, Maker gives SHARES
            Side::Sell => {
                let usdc = (size * price).trunc_with_scale(decimals + LOT_SIZE_SCALE);
                (usdc, size)
            }

            _ => anyhow::bail!("Invalid side: {:?}", side),
        };

        // 3. Generate Salt (IEEE 754 Safe Integer)
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let seconds = now.as_secs_f64();
        let rand = rand::random::<f64>();
        // Matches logic of `to_ieee_754_int`
        let raw_salt = (seconds * rand).round() as u64;
        let salt = raw_salt & ((1 << 53) - 1);

        let mut order = Order::default();
        order.salt = U256::from(salt);
        order.maker = self.funder;
        order.taker = Address::ZERO; // Default taker
        order.tokenId = self.token_id;
        order.makerAmount = U256::from(Self::to_fixed_u128(maker_amount));
        order.takerAmount = U256::from(Self::to_fixed_u128(taker_amount));
        order.side = side as u8;
        order.feeRateBps = self.fee_rate_bps;
        order.nonce = U256::from(0);
        order.signer = self.clob.signer.address();
        order.expiration = U256::from(0);
        order.signatureType = SignatureType::GnosisSafe as u8;

        let signable_order = SignableOrder::builder()
            .order(order)
            .order_type(OrderType::GTC) // FOK is handled via execution options usually, but GTC is standard for constructed limit objects
            .post_only(false)
            .build();

        Ok(signable_order)
    }

    pub async fn execute_order(&self, signable: SignableOrder) -> AppResult<PostOrderResponse> {
        // Sign using the cached signer in CLOB service
        let signed_order = self.clob.client.sign(&self.clob.signer, signable).await?;

        // Post
        let response = self.clob.client.post_order(signed_order).await?;

        if response.success {
            info!("Polymarket Post order response {:?}", response);
            Ok(response)
        } else {
            anyhow::bail!("Order failed: {:?}", response.error_msg);
        }
    }

    // Helper to match SDK math
    fn to_fixed_u128(d: Decimal) -> u128 {
        d.normalize()
            .trunc_with_scale(USDC_DECIMALS)
            .mantissa()
            .to_u128()
            .unwrap_or(0)
    }
}
