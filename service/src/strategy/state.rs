use crate::strategy::book::Orderbook;
use std::sync::Arc;
use tokio::sync::RwLock;

#[derive(Debug, Default)]
pub struct MarketStateInner {
    pub pm_yes: Orderbook,
    pub pm_no: Orderbook,
    pub k_yes: Orderbook,
    pub k_no: Orderbook,
}

#[derive(Debug, Clone, Default)]
pub struct MarketState {
    inner: Arc<RwLock<MarketStateInner>>,
}

impl MarketState {
    pub fn new() -> Self {
        Self {
            inner: Arc::new(RwLock::new(MarketStateInner::default())),
        }
    }

    pub async fn read(&self) -> tokio::sync::RwLockReadGuard<'_, MarketStateInner> {
        self.inner.read().await
    }

    pub async fn write(&self) -> tokio::sync::RwLockWriteGuard<'_, MarketStateInner> {
        self.inner.write().await
    }
}
