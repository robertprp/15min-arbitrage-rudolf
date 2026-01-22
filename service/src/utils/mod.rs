pub(crate) fn get_floored_15min_timestamp(timestamp: i64) -> i64 {
    const WINDOW_SECONDS: i64 = 15 * 60; // 900 seconds
    (timestamp / WINDOW_SECONDS) * WINDOW_SECONDS
}
