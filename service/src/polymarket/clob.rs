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
use polymarket_client_sdk::clob::types::{
    Amount, Order, OrderType, Side, SignableOrder, SignatureType,
};
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

    pub async fn post_market_order(
        &self,
        token_id: U256,
        side: Side,
        amount: Amount,
    ) -> AppResult<PostOrderResponse> {
        let signable_order = self
            .client
            .market_order()
            .token_id(token_id)
            .side(side)
            .amount(amount)
            .build()
            .await
            .context("Failed to build market order")?;

        let signed_order = self
            .client
            .sign(&self.signer, signable_order)
            .await
            .context("Failed to sign order")?;

        let response = self
            .client
            .post_order(signed_order)
            .await
            .context("Failed to create market order")?;

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

    /// Build a market order where the size is specified in SHARES (for both Buy and Sell)
    pub fn build_market_order_with_shares(
        &self,
        raw_price: Decimal,
        shares: Decimal,
        side: Side,
    ) -> AppResult<SignableOrder> {
        let decimals = self.tick_size_decimal.scale();
        let (price, _) = self.validate_price(raw_price)?;

        // Calculate Maker/Taker amounts for SHARES input
        let (taker_amount, maker_amount) = match side {
            // Buy N shares:
            // Taker gets SHARES, Maker gives USDC (calculated)
            Side::Buy => {
                let usdc = (shares * price).trunc_with_scale(decimals + LOT_SIZE_SCALE);
                (shares, usdc)
            }
            // Sell N shares:
            // Taker gets USDC (calculated), Maker gives SHARES
            Side::Sell => {
                let usdc = (shares * price).trunc_with_scale(decimals + LOT_SIZE_SCALE);
                (usdc, shares)
            }
            _ => anyhow::bail!("Invalid side for market order"),
        };

        self.construct_signable_order(maker_amount, taker_amount, side, OrderType::GTC)
    }

    /// Build a market order where the size is specified in USDC (Only valid for Buy)
    pub fn build_market_order_with_usdc(
        &self,
        raw_price: Decimal,
        usdc_amount: Decimal, // We should pass as 1.4 USDC
        side: Side,
    ) -> AppResult<SignableOrder> {
        let decimals = self.tick_size_decimal.scale();
        let (price, _) = self.validate_price(raw_price)?;

        // Validate Side (Cannot Sell by USDC amount)
        if matches!(side, Side::Sell) {
            anyhow::bail!("Sell Orders must specify their amount in shares, not USDC");
        }

        // Calculate Maker/Taker amounts for USDC input
        // Buy: Spend exact USDC to buy calculated Shares
        // Taker gets SHARES (calculated), Maker gives USDC
        let shares = (usdc_amount / price).trunc_with_scale(decimals + LOT_SIZE_SCALE);
        let taker_amount = shares;
        let maker_amount = usdc_amount;

        self.construct_signable_order(maker_amount, taker_amount, side, OrderType::GTC)
    }

    // --- Private Helpers ---

    /// Helper to validate price against tick size
    fn validate_price(&self, raw_price: Decimal) -> AppResult<(Decimal, u32)> {
        let decimals = self.tick_size_decimal.scale();
        let min_tick_size = self.tick_size_decimal;
        let price = raw_price.trunc_with_scale(decimals);

        if price < min_tick_size || price > Decimal::ONE - min_tick_size {
            anyhow::bail!(
                "Price {} is too small or too large for the minimum tick size {}",
                price,
                min_tick_size
            );
        }
        Ok((price, decimals))
    }

    /// Helper to construct the final object (Salt, Struct, Builder)
    fn construct_signable_order(
        &self,
        maker_amount: Decimal,
        taker_amount: Decimal,
        side: Side,
        order_type: OrderType,
    ) -> AppResult<SignableOrder> {
        // Generate Salt
        let now = SystemTime::now().duration_since(UNIX_EPOCH).unwrap();
        let seconds = now.as_secs_f64();
        let rand = rand::random::<f64>();
        let raw_salt = (seconds * rand).round() as u64;
        let salt = raw_salt & ((1 << 53) - 1);

        let mut order = Order::default();
        order.salt = U256::from(salt);
        order.maker = self.funder;
        order.taker = Address::ZERO;
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
            .order_type(order_type)
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
            Ok(response)
        } else {
            anyhow::bail!("Order failed: {:?}", response.error_msg);
        }
    }

    /// This function creates, signs and send all using polymarket sdk. it should be quite low since the sdk does lots of request to clob
    pub async fn create_and_send_high_latency_market_order(&self, token_id: U256, amount: Amount, side: Side) -> AppResult<PostOrderResponse> {
        let signable_order = self.clob
            .client
            .market_order()
            .token_id(token_id)
            .amount(amount)
            .side(side)
            .build().await?;

        let signed_order = self.clob.client.sign(&self.clob.signer, signable_order).await?;
        
        let response = self.clob.client.post_order(signed_order).await?;

        if response.success {
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
