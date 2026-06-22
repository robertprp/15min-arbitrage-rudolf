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
<<<<<<< Updated upstream
            max_positions: 5,
=======
            max_positions: 1,
>>>>>>> Stashed changes
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
<<<<<<< Updated upstream
        if self.last_poly_underlying_price.is_none() {
            return false;
        }

        if self.last_kalshi_underlying_price.is_none() {
            return false;
        }

=======
        if self.last_poly_underlying_price.is_none() || self.last_kalshi_underlying_price.is_none()
        {
            return false;
        }

>>>>>>> Stashed changes
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

<<<<<<< Updated upstream
=======
        // Don't enter new positions if we are already in the "Unwind Window" (last 3 mins)
        if minutes_remaining <= 3 {
            return false;
        }

>>>>>>> Stashed changes
        if current_positions >= self.max_positions {
            return false;
        }

<<<<<<< Updated upstream
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
=======
        // 2. Strike Gap Logic ("Death Zone" Prevention)
        let poly_strike =
            self.poly_metadata.strike_price + self.last_kalshi_underlying_price.unwrap();
        let kalshi_strike =
            self.kalshi_metadata.strike_price + self.last_kalshi_underlying_price.unwrap();
        let kalshi_side = opp.kalshi_side.as_str();

        if poly_strike > kalshi_strike {
            // 0.00008719
            // SCENARIO A: Poly High, Kalshi Low.
            // Gap: Between Kalshi and Poly.
            // Safe Bet: > Kalshi (Yes) AND < Poly (No).
            // Logic: We MUST be buying Kalshi YES (which implies Shorting Poly NO via the Processor logic).
            if kalshi_side != "yes" {
                let ratio = poly_strike / kalshi_strike;
                let v = Decimal::from_str("1.000087").unwrap();

                if ratio > v { return false } else { return true }
            }
        } else if kalshi_strike > poly_strike {
            // SCENARIO B: Poly Low, Kalshi High.
            // Gap: Between Poly and Kalshi.
            // Safe Bet: > Poly (Yes) AND < Kalshi (No).
            // Logic: We MUST be buying Kalshi NO (which implies Longing Poly YES via the Processor logic).
            if kalshi_side != "no" {
                let ratio = kalshi_strike / poly_strike;
                let v = Decimal::from_str("1.000087").unwrap();

                if ratio > v {
                    return false;
                } else {
                    return true;
                }
>>>>>>> Stashed changes
            }
        }

        // If we passed the check, it is safe to enter
        true
    }
}

impl RiskLayer<ActionExit> {
<<<<<<< Updated upstream
    pub fn should_unwind(&self) -> bool {
        if !self.has_fresh_data() {
            return false;
        }
=======
    pub fn check_exit_strategy(&self) -> ExitRecommendation {
        // Holding since we should cover death zone already
        return ExitRecommendation::Hold;
>>>>>>> Stashed changes

        let seconds_remaining = self.get_seconds_remaining();

<<<<<<< Updated upstream
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
=======
        // 1. Calculate Distance to Strike (Risk Factor)
        let is_danger_zone = if let Some(current_price) = self.last_poly_underlying_price {
            let strike = self.poly_metadata.strike_price;

            // Calculate absolute percentage distance
            // e.g., Price 100, Strike 100 -> 0% (Danger)
            // e.g., Price 101, Strike 100 -> 1% (Safe)
            let distance_pct = (current_price - strike).abs() / strike;

            // Threshold: 0.03% (e.g., $30 on a $100k asset)
            // If we are closer than this, we are in the Danger Zone.
            distance_pct < dec!(0.0002)
        } else {
            warn!("Exiting as we dont have price delta");
            true
        };

        // 2. Adjust Strategy based on Zone
        if is_danger_zone {
            if seconds_remaining > 600 {
                // 10+ mins: Plenty of time, hold for target.
                return ExitRecommendation::ExitWithProfit(dec!(0.12));
            } else if seconds_remaining > 480 {
                // 8-10 mins: Slight decay, lower expectation slightly.
                return ExitRecommendation::ExitWithProfit(dec!(0.11));
            } else if seconds_remaining > 360 {
                // 6-8 mins: Still healthy, but recognize time passage.
                return ExitRecommendation::ExitWithProfit(dec!(0.10));
            } else if seconds_remaining > 240 {
                // 4-6 mins: Mid-game. tightening up.
                return ExitRecommendation::ExitWithProfit(dec!(0.09));
            } else if seconds_remaining > 180 {
                // 3-4 mins: Risk increasing.
                return ExitRecommendation::ExitWithProfit(dec!(0.08));
            } else if seconds_remaining > 150 {
                // 2.5 - 3 mins: Approaching the danger zone.
                return ExitRecommendation::ExitWithProfit(dec!(0.07));
            } else if seconds_remaining > 120 {
                // 2 mins: significant Theta decay often kicks in here.
                return ExitRecommendation::ExitWithProfit(dec!(0.06));
            } else if seconds_remaining > 100 {
                // ~1.5 mins: Just get a base hit.
                return ExitRecommendation::ExitWithProfit(dec!(0.05));
            } else if seconds_remaining > 80 {
                // ~1.3 mins: Take small profit.
                return ExitRecommendation::ExitWithProfit(dec!(0.04));
            } else if seconds_remaining > 70 {
                // ~1.1 mins: Skimming.
                return ExitRecommendation::ExitWithProfit(dec!(0.02));
            } else if seconds_remaining > 60 {
                // Last minute buffer: Scratch trade (0.0). Get out break-even.
                return ExitRecommendation::ExitWithProfit(Decimal::ZERO);
            } else {
                // < 60 seconds: Liquidity risk is too high.
                warn!("🚨 Risk Layer: ATM Danger Zone + Low Time (<60s). Panic Exit.");
                return ExitRecommendation::PanicExit;
            }
        } else {
            return ExitRecommendation::Hold;
            // if seconds_remaining > 600 {
            // 10+ mins: Plenty of time, hold for target.
            // return ExitRecommendation::ExitWithProfit(dec!(0.15));
            // } else if seconds_remaining > 480 {
            // 8-10 mins: Slight decay, lower expectation slightly.
            // return ExitRecommendation::ExitWithProfit(dec!(0.14));
            // } else if seconds_remaining > 360 {
            // 6-8 mins: Still healthy, but recognize time passage.
            // return ExitRecommendation::ExitWithProfit(dec!(0.13));
            // } else {
            // }
>>>>>>> Stashed changes
        }

        false
    }
}
