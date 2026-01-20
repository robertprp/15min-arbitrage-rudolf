use std::str::FromStr;
use std::sync::Arc;

use alloy::primitives::Address;
use alloy::signers::{k256::ecdsa::SigningKey, local::LocalSigner};
use alloy::signers::Signer;
use anyhow::Context;
use async_trait::async_trait;
use polymarket_client_sdk::auth::Normal;
use polymarket_client_sdk::{
    auth::state::Authenticated,
    clob::{
        types::{OrderType, Side},
        Client, Config,
    },
    types::{Decimal, U256},
    POLYGON,
};
use tracing::{debug, info};

use crate::{
    config::service::ConfigService,
    services::{ServiceFactory, ServiceProvider},
};
use lib::AppResult;

pub struct PolymarketClobService {
    // We hold the client in an Authenticated state with the LocalSigner generic
    client: Client<Authenticated<Normal>>,
    // We must hold the signer separately to pass it to the .sign() method
    // per the SDK design pattern.
    signer: LocalSigner<SigningKey>,
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
            .signature_type(polymarket_client_sdk::clob::types::SignatureType::GnosisSafe)
            .funder(Address::from_str(&funder)?)
            .authenticate()
            .await
            .context("Failed to authenticate Polymarket CLOB client")?;

        info!("Polymarket CLOB Service authenticated successfully");

        Ok(Self { client, signer })
    }

    /// Places a "Fill or Kill" (FOK) Limit Order.
    /// If the order cannot be completely filled immediately, it is cancelled.
    pub async fn place_fok_order(
        &self,
        token_id: U256,
        price: Decimal,
        size: Decimal,
        side: u8,
    ) -> AppResult<String> {
        debug!(
            "Placing FOK Order: Token: {}, Price: {}, Size: {}, Side: {:?}",
            token_id, price, size, side
        );

        // 1. Build the Limit Order with OrderType::FOK
        let limit_order_builder = self
            .client
            .limit_order()
            .token_id(token_id)
            .price(price)
            .size(size)
            .side(Side::try_from(side)?)
            .order_type(OrderType::GTC); // Crucial for Fill-or-Kill

        let limit_order = limit_order_builder
            .build()
            .await
            .context("Failed to build FOK limit order")?;

        // 2. Sign the Order (EIP-712)
        let signed_order = self
            .client
            .sign(&self.signer, limit_order)
            .await
            .context("Failed to sign FOK order")?;

        // 3. Post to CLOB
        let response = self
            .client
            .post_order(signed_order)
            .await
            .context("Failed to post FOK order to CLOB")?;

        if response.success {
            info!(
                "FOK Order Placed Successfully. Order ID: {}",
                response.order_id
            );
            Ok(response.order_id)
        } else {
            // Even if the HTTP request worked, the matching engine might reject it
            // (e.g., if FOK couldn't be filled)
            let err_msg = format!(
                "Order placement failed (Logic): OrderID: {}, Info: {:?}",
                response.order_id, response.error_msg
            );
            anyhow::bail!(err_msg);
        }
    }

    // pub async fn cancel_order(&self, order_id: &str) -> AppResult<()> {
    //     let response = self
    //         .client
    //         .cancel_order(order_id)
    //         .await
    //         .context("Failed to cancel order")?;

    //     if response.canceled {
    //         info!("Order {} canceled successfully", order_id);
    //         Ok(())
    //     } else {
    //         anyhow::bail!("Failed to cancel order: {}", order_id);
    //     }
    // }
}

#[async_trait]
impl ServiceFactory for PolymarketClobService {
    async fn factory(services: ServiceProvider) -> AppResult<Self> {
        let config_service = services.get_service_unchecked::<ConfigService>().await;
        Self::new(config_service).await
    }
}