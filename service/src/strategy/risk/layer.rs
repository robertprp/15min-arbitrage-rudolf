// File: service/src/strategy/risk/layer.rs

use crate::market::exchange::{KalshiExchange, PolymarketExchange};
use crate::market::metadata::MarketMetadata;
use crate::strategy::market::Opportunity;
use chrono::{DateTime, Timelike, Utc};
use rust_decimal::Decimal;
use std::marker::PhantomData;
use std::str::FromStr;
use tracing::{info, warn};

// --- Action Markers ---
#[derive(Clone, Debug)]
pub struct ActionEnter;

#[derive(Clone, Debug)]
pub struct ActionExit;

#[derive(Clone)]
pub struct RiskLayer<Action> {
    // Static Metadata
    pub poly_metadata: MarketMetadata<PolymarketExchange>,
    pub kalshi_metadata: MarketMetadata<KalshiExchange>,

    // Dynamic State (Underlying Asset Prices)
    pub last_poly_underlying_price: Option<Decimal>,
    pub last_kalshi_underlying_price: Option<Decimal>,

    pub last_poly_update: Option<DateTime<Utc>>,
    pub last_kalshi_update: Option<DateTime<Utc>>,

    // Marker for the generic state
    _marker: PhantomData<Action>,

    // Config
    min_minutes_to_expiry: u32,
    max_positions: u32,
}

// --- Common Implementation (Getters/Setters) ---
impl<A> RiskLayer<A> {
    pub fn new(
        poly_metadata: MarketMetadata<PolymarketExchange>,
        kalshi_metadata: MarketMetadata<KalshiExchange>,
    ) -> Self {
        Self {
            poly_metadata,
            kalshi_metadata,
            last_poly_underlying_price: None,
            last_kalshi_underlying_price: None,
            last_poly_update: None,
            last_kalshi_update: None,
            _marker: PhantomData,
            min_minutes_to_expiry: 2,
            max_positions: 5,
        }
    }

    pub fn update_poly_price(&mut self, price: Decimal) {
        self.last_poly_underlying_price = Some(price);
        self.last_poly_update = Some(Utc::now());
    }

    pub fn update_kalshi_price(&mut self, price: Decimal) {
        self.last_kalshi_underlying_price = Some(price);
        self.last_kalshi_update = Some(Utc::now());
    }

    fn has_fresh_data(&self) -> bool {
        if self.last_poly_underlying_price.is_none() {
            return false;
        }

        if self.last_kalshi_underlying_price.is_none() {
            return false;
        }

        true
    }

    /// Helper to get seconds remaining in the current 15-minute window
    fn get_seconds_remaining(&self) -> i64 {
        let now = Utc::now();
        let current_minute = now.minute();
        let current_second = now.second();

        // Minutes passed in the current 15m window (0..14)
        let minutes_passed = current_minute % 15;

        // Total seconds passed in this window
        let seconds_passed = (minutes_passed as i64 * 60) + current_second as i64;

        // Total seconds in a 15m window = 900
        let total_window_seconds = 15 * 60;

        total_window_seconds - seconds_passed
    }

    // Kept for ActionEnter compatibility
    fn get_minutes_remaining(&self) -> u32 {
        (self.get_seconds_remaining() / 60) as u32
    }
}

// --- Logic for ENTERING a position ---
impl RiskLayer<ActionEnter> {
    pub fn is_safe_to_enter(&self, opp: &Opportunity, current_positions: u32) -> bool {
        // 1. Basic Data Availability Checks
        if !self.has_fresh_data() {
            return false;
        }

        // 2. Position Limits & Time to Expiry Checks
        let minutes_remaining = self.get_minutes_remaining();

        if current_positions >= self.max_positions {
            return false;
        }

        if minutes_remaining <= self.min_minutes_to_expiry {
            return false;
        }

        // 3. Unwrap Prices (Safe because of has_fresh_data check above)
        let k_price = self.last_kalshi_underlying_price.unwrap();
        let pm_price = self.last_poly_underlying_price.unwrap();

        let k_strike = self.kalshi_metadata.strike_price;
        let pm_strike = self.poly_metadata.strike_price;

        // 4. Define Threshold (0.06% = 0.0005)
        // This allows for small tracking errors between the two indices
        // Max deviation is 0.06%
        let max_deviation = Decimal::new(6, 4);

        // let strike_price_dist = (pm_strike - k_strike).abs();
        // let price_dist = (pm_price - k_price).abs();
        // Distance = Underlying - Strike
        let pm_dist = pm_price - pm_strike;
        let k_dist = k_price - k_strike;

        // The "Net Skew" represents how differently the two markets view the asset's position relative to the strike.
        let net_skew = (pm_dist - k_dist).abs();

        if !pm_price.is_zero() {
            let skew_ratio = net_skew / pm_price;

            if skew_ratio > max_deviation {
                warn!(
                    "🛑 RISK REJECT: Market correlation broken. Poly Dist: {}, Kalshi Dist: {}, Skew: {:.4}%",
                    pm_dist,
                    k_dist,
                    skew_ratio * Decimal::from(100)
                );

                // Optional: Log the raw values for debugging
                info!(
                    "Risk Context -> PM(S: {}, K: {}), Kalshi(S: {}, K: {})",
                    pm_price, pm_strike, k_price, k_strike
                );

                return false;
            }
        }

        // If we passed the check, it is safe to enter
        true
    }
}

impl RiskLayer<ActionExit> {
    pub fn should_unwind(&self) -> bool {
        if !self.has_fresh_data() {
            return false;
        }

        let seconds_remaining = self.get_seconds_remaining();

        let poly_price = self.last_poly_underlying_price.unwrap();
        let kalshi_price = self.last_kalshi_underlying_price.unwrap();

        let poly_strike = self.poly_metadata.strike_price;
        let kalshi_strike = self.kalshi_metadata.strike_price;

        let poly_dist_pct = (poly_price - poly_strike).abs() / poly_strike;
        let kalshi_dist_pct = (kalshi_price - kalshi_strike).abs() / kalshi_strike;

        // TODO: Think if we need to be more strict here.
        // We should also check for last underlying price updated time on both.
        // Constants for thresholds
        let pct_0_05 = Decimal::new(4, 4); // 0.0005 (0.04%)

        let unwind_secs = 60;
        if seconds_remaining < unwind_secs {
            if poly_dist_pct > pct_0_05 && kalshi_dist_pct > pct_0_05 {
                // correct usage with the tracing crate
                info!("🛡️ Risk Layer (Exit): Holding. Price is safely away from strike.");
                info!(%seconds_remaining, %poly_dist_pct, %kalshi_dist_pct, %poly_strike, %kalshi_strike, %poly_price, %kalshi_price);

                return false;
            } else {
                info!(
                    "[ALERT] We should unwind. less than {} seconds left and price is within {}% of strike.",
                    unwind_secs,
                    pct_0_05 * Decimal::ONE_HUNDRED
                );

                info!(%seconds_remaining, %poly_dist_pct, %kalshi_dist_pct, %poly_strike, %kalshi_strike, %poly_price, %kalshi_price);

                return true;
            }
        }

        false
    }
}
