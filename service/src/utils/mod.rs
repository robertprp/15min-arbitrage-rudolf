use chrono::{DateTime, Timelike, Utc};

pub(crate) fn get_floored_15min_timestamp(timestamp: i64) -> i64 {
    const WINDOW_SECONDS: i64 = 15 * 60; // 900 seconds
    (timestamp / WINDOW_SECONDS) * WINDOW_SECONDS
}

/// Returns the duration until the next 15-minute window starts.
/// e.g. if it is 12:05:00, this returns 10 minutes (Duration).
pub fn get_duration_until_next_window() -> std::time::Duration {
    let now = Utc::now();
    let current_minute = now.minute();
    let current_second = now.second();
    let nanos = now.nanosecond();

    // Calculate how many minutes into the current 15m window we are
    let minutes_passed = current_minute % 15;
    
    // Total seconds passed in this window
    let seconds_passed = (minutes_passed as u64 * 60) + current_second as u64;
    let nanos_passed = nanos as u64;

    let total_window_seconds = 15 * 60;
    
    // Calculate remaining seconds
    let remaining_seconds = total_window_seconds - seconds_passed;
    
    // Subtract nanos to be precise (avoid overshooting)
    if remaining_seconds == 0 {
        return std::time::Duration::from_secs(0);
    }

    std::time::Duration::from_secs(remaining_seconds) 
        - std::time::Duration::from_nanos(nanos_passed)
}