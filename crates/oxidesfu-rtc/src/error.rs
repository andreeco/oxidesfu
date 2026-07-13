/// Result type for OxideSFU RTC operations.
pub type RtcResult<T> = Result<T, Box<dyn std::error::Error + Send + Sync>>;
