use anyhow::{Context, Result, bail};
use async_trait::async_trait;
use serde::{Deserialize, Serialize};
use std::{env, fs::read_to_string, ops::Deref, path::Path, sync::Arc};

use crate::services::{ServiceFactory, ServiceProvider};

#[derive(Debug, Serialize, Deserialize)]
pub struct ConfigServiceInner {
    pub chain: ChainConfig,
    pub polymarket: PolymarketConfig,
    pub kalshi: KalshiConfig,
}

#[derive(Debug, Clone, Serialize, Deserialize, Default)]
pub struct ChainConfig {
    pub chain_id: u64,
    pub private_key: String,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct PolymarketConfig {
    pub api_key: String,
    pub api_secret: String,
    pub api_passphrase: String,
    pub funder: String,
}

#[derive(Default, Debug, Clone, Serialize, Deserialize)]
pub struct KalshiConfig {
    pub api_key_id: String,
    pub private_key_pem: String,
}

#[derive(Debug, Clone, Serialize)]
pub struct ConfigService(Arc<ConfigServiceInner>);

impl Deref for ConfigService {
    type Target = ConfigServiceInner;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[buildstructor::buildstructor]
impl ConfigService {
    #[builder]
    pub fn new(
        chain: Option<ChainConfig>,
        polymarket: Option<PolymarketConfig>,
        kalshi: Option<KalshiConfig>,
    ) -> Result<Self> {
        let inner = ConfigServiceInner {
            chain: chain.unwrap_or_default(),
            polymarket: polymarket.unwrap_or_default(),
            kalshi: kalshi.unwrap_or_default(),
        };
        Ok(ConfigService(Arc::new(inner)))
    }

    /// Loads configuration specifically from System ENV or .env file
    pub fn from_env() -> Result<Self> {
        // Load .env file if it exists, ignore if missing (relies on real env vars)
        dotenvy::dotenv().ok();

        // --- Chain Configuration ---
        let chain_id = env::var("CHAIN_ID")
            .context("CHAIN_ID environment variable not set")?
            .parse::<u64>()
            .context("CHAIN_ID must be a u64")?;

        let private_key =
            env::var("PRIVATE_KEY").context("PRIVATE_KEY environment variable not set")?;

        let chain = ChainConfig {
            chain_id,
            private_key,
        };

        let api_key = env::var("POLYMARKET_API_KEY")
            .context("POLYMARKET_API_KEY environment variable not set")?;

        let api_secret = env::var("POLYMARKET_API_SECRET")
            .context("POLYMARKET_API_SECRET missing (required if API_KEY is set)")?;
        let api_passphrase = env::var("POLYMARKET_API_PASSPHRASE")
            .context("POLYMARKET_API_PASSPHRASE missing (required if API_KEY is set)")?;
        let funder = env::var("POLYMARKET_FUNDER")
            .context("POLYMARKET_ADDRESS missing (required if API_KEY is set)")?;

        let polymarket = PolymarketConfig {
            api_key,
            api_secret,
            api_passphrase,
            funder,
        };

        let kalshi_api_key =
            env::var("KALSHI_API_KEY_ID").context("KALSHI_API_KEY environment variable not set")?;

        let kalshi_pem_file_path_var = env::var("KALSHI_PRIVATE_KEY_PEM")
            .context("KALSHI_PRIVATE_KEY_PEM environment variable not set")?;

        let private_key_pem_path = Path::new(&kalshi_pem_file_path_var);

        let private_key_pem =
            read_to_string(private_key_pem_path).context("Failed to open private key PEM file")?;

        let kalshi = KalshiConfig {
            api_key_id: kalshi_api_key,
            private_key_pem,
        };

        // --- Kalshi Configuration ---
        let inner = ConfigServiceInner {
            chain,
            polymarket,
            kalshi,
        };

        Ok(ConfigService(Arc::new(inner)))
    }
}

#[async_trait]
impl ServiceFactory for ConfigService {
    async fn factory(_services: ServiceProvider) -> Result<Self> {
        // Try to load from ENV first
        match ConfigService::from_env() {
            Ok(cfg) => Ok(cfg),
            Err(e) => {
                bail!("Could not load configuration from ENV: {:#}", e)
            }
        }
    }
}
