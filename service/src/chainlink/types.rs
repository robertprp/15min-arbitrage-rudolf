use serde::Deserialize;

use crate::market::symbols::Min15Symbols;

pub const CHAINLINK_BTC_PRICE_FEED_ID: &str =
    "0x00039d9e45394f473ab1f050a1b963e6b05351e52d71e507509ada0c95ed75b8";

pub enum ChainLinkAsset {
    BTCUSD,
    ETHUSD,
}

impl ToString for ChainLinkAsset {
    fn to_string(&self) -> String {
        match self {
            ChainLinkAsset::BTCUSD => "btc/usd".to_string(),
            ChainLinkAsset::ETHUSD => "eth/usd".to_string(),
        }
    }
}

pub trait ToChainlinkAsset {
    fn to_chainlink_asset(&self) -> ChainLinkAsset;
}

impl ToChainlinkAsset for Min15Symbols {
    fn to_chainlink_asset(&self) -> ChainLinkAsset {
        match self {
            Min15Symbols::BTC => ChainLinkAsset::BTCUSD,
            Min15Symbols::ETH => ChainLinkAsset::ETHUSD,
        }
    }
}

// --- Internal Serde Structs for API Response ---
#[derive(Debug, Deserialize)]
pub struct ChainlinkResponse {
    pub data: ChainlinkData,
}

#[derive(Debug, Deserialize)]
pub struct ChainlinkData {
    #[serde(rename = "mercuryHistory15MinMarkers")]
    pub mercury_history: MercuryHistory,
}

#[derive(Debug, Deserialize)]
pub struct MercuryHistory {
    pub nodes: Vec<ChainlinkNode>,
}

#[derive(Debug, Deserialize)]
pub struct ChainlinkNode {
    #[serde(rename = "timeBucket")]
    pub time_bucket: String,
    pub open: String,
}
