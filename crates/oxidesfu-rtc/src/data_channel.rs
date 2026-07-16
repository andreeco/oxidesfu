use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU32, Ordering},
    },
    time::{Duration, Instant},
};

use bytes::BytesMut;
use webrtc::data_channel::{
    DataChannel as WebRtcDataChannel, DataChannelEvent, DetachedDataChannel, RTCDataChannelState,
};

use crate::RtcResult;

/// OxideSFU-owned wrapper around a `webrtc-rs` data channel.
#[derive(Clone)]
pub struct DataChannel {
    pub(crate) inner: Arc<dyn WebRtcDataChannel>,
    detached: Arc<tokio::sync::Mutex<Option<Arc<dyn DetachedDataChannel>>>>,
    buffered_amount_high: Arc<AtomicBool>,
    slow_reader_bitrate_threshold_bps: Arc<AtomicU32>,
    slow_reader_threshold_configured: Arc<AtomicBool>,
    send_rate_samples: Arc<tokio::sync::Mutex<SendRateSamples>>,
}

const RELIABLE_SLOW_READER_WRITE_TIMEOUT: Duration = Duration::from_millis(50);
const RELIABLE_SLOW_READER_BITRATE_WINDOW: Duration = Duration::from_secs(2);
const RELIABLE_SLOW_READER_MIN_BITRATE_SAMPLE_WINDOW: Duration = Duration::from_millis(100);
const UNRELIABLE_TARGET_LATENCY: Duration = Duration::from_millis(100);
const UNRELIABLE_MIN_BUFFERED_AMOUNT: u64 = 2_000;

#[derive(Clone, Copy)]
struct SendRateWindow {
    start: Instant,
    bytes: usize,
}

struct SendRateSamples {
    bytes_total: usize,
    windows: VecDeque<SendRateWindow>,
    active: SendRateWindow,
    last_buffered_amount: u64,
    start: Instant,
    initialized: bool,
}

impl SendRateSamples {
    fn at(now: Instant) -> Self {
        Self {
            bytes_total: 0,
            windows: VecDeque::new(),
            active: SendRateWindow {
                start: now,
                bytes: 0,
            },
            last_buffered_amount: 0,
            start: now,
            initialized: false,
        }
    }
}

impl Default for SendRateSamples {
    fn default() -> Self {
        Self::at(Instant::now())
    }
}

impl SendRateSamples {
    fn add_sample(&mut self, now: Instant, bytes: usize, buffered_amount: u64) {
        if !self.initialized {
            *self = Self::at(now);
            self.initialized = true;
        }

        let buffered_delta = buffered_amount as i128 - self.last_buffered_amount as i128;
        // Match upstream datachannel bitrate semantics: bytes delivered in this sample are
        // written bytes adjusted by buffered-amount movement. When buffered amount grows,
        // fewer bytes were delivered; when it shrinks, drained bytes are counted as delivered.
        let delivered_bytes = (bytes as i128 - buffered_delta).max(0) as usize;
        self.last_buffered_amount = buffered_amount;

        if now.saturating_duration_since(self.active.start)
            >= RELIABLE_SLOW_READER_MIN_BITRATE_SAMPLE_WINDOW
        {
            let previous = std::mem::replace(
                &mut self.active,
                SendRateWindow {
                    start: now,
                    bytes: 0,
                },
            );
            self.windows.push_back(previous);

            while self.windows.front().is_some_and(|window| {
                now.saturating_duration_since(window.start)
                    > RELIABLE_SLOW_READER_BITRATE_WINDOW
                        + RELIABLE_SLOW_READER_MIN_BITRATE_SAMPLE_WINDOW
            }) {
                if let Some(window) = self.windows.pop_front() {
                    self.bytes_total = self.bytes_total.saturating_sub(window.bytes);
                }
            }

            if let Some(window) = self.windows.front() {
                self.start = window.start;
            } else {
                self.start = now;
                self.bytes_total = 0;
            }
        }

        self.bytes_total = self.bytes_total.saturating_add(delivered_bytes);
        self.active.bytes = self.active.bytes.saturating_add(delivered_bytes);
    }

    fn bitrate_bps(&self, now: Instant) -> Option<u32> {
        if !self.initialized {
            return None;
        }

        let elapsed = now.saturating_duration_since(self.start);
        if elapsed < RELIABLE_SLOW_READER_MIN_BITRATE_SAMPLE_WINDOW {
            return None;
        }

        let elapsed_millis = elapsed.as_millis();
        if elapsed_millis == 0 {
            return None;
        }

        let bps = (self.bytes_total as u128)
            .saturating_mul(8)
            .saturating_mul(1_000)
            .saturating_div(elapsed_millis);
        Some(bps.min(u128::from(u32::MAX)) as u32)
    }
}

impl DataChannel {
    pub(crate) fn new(inner: Arc<dyn WebRtcDataChannel>) -> Self {
        Self {
            inner,
            detached: Arc::new(tokio::sync::Mutex::new(None)),
            buffered_amount_high: Arc::new(AtomicBool::new(false)),
            slow_reader_bitrate_threshold_bps: Arc::new(AtomicU32::new(0)),
            slow_reader_threshold_configured: Arc::new(AtomicBool::new(false)),
            send_rate_samples: Arc::new(tokio::sync::Mutex::new(SendRateSamples::default())),
        }
    }

    async fn sample_send_rate_bps(&self, bytes: usize, buffered_amount: u64) -> Option<u32> {
        let now = Instant::now();
        let mut samples = self.send_rate_samples.lock().await;
        samples.add_sample(now, bytes, buffered_amount);
        samples.bitrate_bps(now)
    }

    async fn current_send_rate_bps(&self) -> Option<u32> {
        let now = Instant::now();
        let samples = self.send_rate_samples.lock().await;
        samples.bitrate_bps(now)
    }

    async fn detached_writer(&self) -> Option<Arc<dyn DetachedDataChannel>> {
        {
            let detached = self.detached.lock().await;
            if let Some(existing) = detached.as_ref() {
                return Some(existing.clone());
            }
        }

        match self.inner.detach_with_deadline().await {
            Ok(detached) => {
                let mut detached_slot = self.detached.lock().await;
                *detached_slot = Some(detached.clone());
                Some(detached)
            }
            Err(webrtc::error::Error::ErrDetachNotEnabled)
            | Err(webrtc::error::Error::ErrDetachBeforeOpened) => None,
            Err(_) => None,
        }
    }
}

impl DataChannel {
    /// Returns this data channel's label.
    pub async fn label(&self) -> RtcResult<String> {
        Ok(self.inner.label().await?)
    }

    /// Returns whether this data channel guarantees in-order delivery.
    pub async fn ordered(&self) -> RtcResult<bool> {
        Ok(self.inner.ordered().await?)
    }

    /// Returns the maximum retransmit count if configured.
    pub async fn max_retransmits(&self) -> RtcResult<Option<u16>> {
        Ok(self.inner.max_retransmits().await?)
    }

    /// Returns the number of outbound bytes buffered by the WebRTC data channel.
    pub async fn buffered_amount(&self) -> RtcResult<u64> {
        Ok(self.inner.buffered_amount().await?)
    }

    /// Returns whether the outbound buffer has crossed the configured high-water mark.
    pub fn is_buffered_amount_high(&self) -> bool {
        self.buffered_amount_high.load(Ordering::Relaxed)
    }

    /// Sets the high-water mark used to detect backpressured data-channel writes.
    pub async fn set_buffered_amount_high_threshold(&self, threshold: u32) -> RtcResult<()> {
        self.inner
            .set_buffered_amount_high_threshold(threshold)
            .await?;
        Ok(())
    }

    /// Sets the minimum send bitrate required to keep retrying blocked reliable writes.
    ///
    /// When a reliable write remains backpressured and the measured send bitrate falls
    /// below this threshold, [`send_bytes`](Self::send_bytes) returns `WouldBlock` with
    /// LiveKit-compatible slow-reader semantics instead of waiting indefinitely.
    pub fn set_slow_reader_bitrate_threshold_bps(&self, threshold_bps: u32) {
        self.slow_reader_bitrate_threshold_bps
            .store(threshold_bps, Ordering::Relaxed);
        self.slow_reader_threshold_configured
            .store(true, Ordering::Relaxed);
    }

    fn slow_reader_bitrate_threshold_bps(&self, high_threshold: u32) -> Option<u32> {
        if !self
            .slow_reader_threshold_configured
            .load(Ordering::Relaxed)
        {
            return Some(high_threshold);
        }

        let configured = self
            .slow_reader_bitrate_threshold_bps
            .load(Ordering::Relaxed);
        if configured == 0 {
            None
        } else {
            Some(configured)
        }
    }

    /// Sets the low-water mark used to clear backpressured data-channel writes.
    pub async fn set_buffered_amount_low_threshold(&self, threshold: u32) -> RtcResult<()> {
        self.inner
            .set_buffered_amount_low_threshold(threshold)
            .await?;
        Ok(())
    }

    /// Returns whether the data channel is currently open.
    pub async fn is_open(&self) -> RtcResult<bool> {
        Ok(matches!(
            self.inner.ready_state().await?,
            RTCDataChannelState::Open
        ))
    }

    /// Waits until the data channel is open.
    pub async fn wait_open(&self) -> RtcResult<()> {
        while let Some(event) = self.inner.poll().await {
            match event {
                DataChannelEvent::OnOpen => return Ok(()),
                DataChannelEvent::OnClose => {
                    return Err(std::io::Error::other("data channel closed before open").into());
                }
                _ => {}
            }
        }
        Err(std::io::Error::other("data channel event stream ended before open").into())
    }

    /// Sends raw binary data over the data channel.
    pub async fn send_bytes(&self, bytes: &[u8]) -> RtcResult<()> {
        let high_threshold = self.inner.buffered_amount_high_threshold().await?;
        let slow_reader_bitrate_threshold = self.slow_reader_bitrate_threshold_bps(high_threshold);
        let high_threshold_u64 = u64::from(high_threshold);
        let is_unreliable = matches!(self.max_retransmits().await?, Some(0));

        if is_unreliable && let Some(bitrate) = self.current_send_rate_bps().await {
            let buffered_amount = self.inner.buffered_amount().await?;
            let buffered_limit = u64::from(bitrate)
                .saturating_mul(UNRELIABLE_TARGET_LATENCY.as_millis() as u64)
                .saturating_div(8)
                .saturating_div(1_000);
            if buffered_amount > buffered_limit && buffered_amount > UNRELIABLE_MIN_BUFFERED_AMOUNT
            {
                return Err(std::io::Error::new(
                    std::io::ErrorKind::WouldBlock,
                    "data dropped due to high buffered amount",
                )
                .into());
            }
        }

        loop {
            let send_result: Result<usize, webrtc::error::Error> = if let Some(detached) =
                self.detached_writer().await
            {
                detached
                    .set_write_deadline(Some(Instant::now() + RELIABLE_SLOW_READER_WRITE_TIMEOUT))
                    .await;
                detached
                    .write_data_channel(BytesMut::from(bytes), false)
                    .await
            } else {
                self.inner
                    .send_with_timeout(BytesMut::from(bytes), RELIABLE_SLOW_READER_WRITE_TIMEOUT)
                    .await
                    .map(|_| bytes.len())
            };

            let buffered_amount = self.inner.buffered_amount().await?;
            self.buffered_amount_high
                .store(buffered_amount >= high_threshold_u64, Ordering::Relaxed);

            match send_result {
                Ok(bytes_written) => {
                    let _ = self
                        .sample_send_rate_bps(bytes_written, buffered_amount)
                        .await;
                    return Ok(());
                }
                Err(webrtc::error::Error::ErrTimeout) => {
                    if is_unreliable {
                        return Err(webrtc::error::Error::ErrTimeout.into());
                    }

                    let bitrate_bps = self.sample_send_rate_bps(0, buffered_amount).await;
                    let should_retry = match slow_reader_bitrate_threshold {
                        None => return Err(webrtc::error::Error::ErrTimeout.into()),
                        Some(threshold) => bitrate_bps
                            .map(|bitrate| bitrate >= threshold)
                            .unwrap_or(true),
                    };

                    if should_retry {
                        continue;
                    }

                    return Err(std::io::Error::new(
                        std::io::ErrorKind::WouldBlock,
                        "data dropped by slow reader",
                    )
                    .into());
                }
                Err(error) => {
                    return Err(error.into());
                }
            }
        }
    }

    /// Sends a UTF-8 text message over the data channel.
    pub async fn send_text(&self, text: &str) -> RtcResult<()> {
        self.inner.send_text(text).await?;
        Ok(())
    }

    /// Receives the next data-channel message as raw bytes.
    pub async fn recv_bytes(&self) -> RtcResult<Vec<u8>> {
        if let Some(detached) = self.detached_writer().await {
            return detached
                .read_data_channel()
                .await
                .map(|message| message.data.to_vec())
                .ok_or_else(|| {
                    std::io::Error::other("detached data channel ended before message").into()
                });
        }

        while let Some(event) = self.inner.poll().await {
            match event {
                DataChannelEvent::OnBufferedAmountHigh => {
                    self.buffered_amount_high.store(true, Ordering::Relaxed);
                }
                DataChannelEvent::OnBufferedAmountLow => {
                    self.buffered_amount_high.store(false, Ordering::Relaxed);
                }
                DataChannelEvent::OnMessage(message) => return Ok(message.data.to_vec()),
                DataChannelEvent::OnClose => {
                    return Err(std::io::Error::other("data channel closed before message").into());
                }
                _ => {}
            }
        }
        Err(std::io::Error::other("data channel event stream ended before message").into())
    }

    /// Receives the next UTF-8 text message from the data channel.
    pub async fn recv_text(&self) -> RtcResult<String> {
        Ok(String::from_utf8(self.recv_bytes().await?)?)
    }
}

/// Data channel creation options used by OxideSFU.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DataChannelOptions {
    /// Whether data must be delivered in order.
    pub ordered: bool,
    /// Maximum retransmissions for unreliable delivery (`Some(0)` for lossy).
    pub max_retransmits: Option<u16>,
}

impl Default for DataChannelOptions {
    fn default() -> Self {
        Self {
            ordered: true,
            max_retransmits: None,
        }
    }
}

/// Application data channel reliability class.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum DataChannelKind {
    /// Ordered, reliable application data channel.
    Reliable,
    /// Unordered lossy application data channel.
    Lossy,
    /// Data-track frame transport channel.
    DataTrack,
}

#[cfg(test)]
mod tests {
    use std::{
        collections::VecDeque,
        sync::{Arc, Mutex},
        time::{Duration, Instant},
    };

    use bytes::BytesMut;
    use webrtc::data_channel::{
        DataChannel as WebRtcDataChannel, DataChannelEvent, DetachedDataChannel, RTCDataChannelId,
        RTCDataChannelState,
    };

    use super::{
        DataChannel, RELIABLE_SLOW_READER_BITRATE_WINDOW,
        RELIABLE_SLOW_READER_MIN_BITRATE_SAMPLE_WINDOW, SendRateSamples,
    };

    #[derive(Clone)]
    enum SendOutcome {
        Ok(usize),
        Timeout,
    }

    struct MockDataChannelState {
        buffered_amount: u64,
        outcomes: VecDeque<SendOutcome>,
        calls: usize,
    }

    struct MockWebRtcDataChannel {
        state: Mutex<MockDataChannelState>,
        high_threshold: u32,
        max_retransmits: Option<u16>,
    }

    impl MockWebRtcDataChannel {
        fn set_buffered_amount(&self, value: u64) {
            if let Ok(mut state) = self.state.lock() {
                state.buffered_amount = value;
            }
        }

        fn call_count(&self) -> usize {
            self.state
                .lock()
                .map(|state| state.calls)
                .unwrap_or_default()
        }
    }

    #[async_trait::async_trait]
    impl WebRtcDataChannel for MockWebRtcDataChannel {
        async fn label(&self) -> webrtc::error::Result<String> {
            Ok("mock".to_string())
        }

        async fn ordered(&self) -> webrtc::error::Result<bool> {
            Ok(true)
        }

        async fn max_packet_life_time(&self) -> webrtc::error::Result<Option<u16>> {
            Ok(None)
        }

        async fn max_retransmits(&self) -> webrtc::error::Result<Option<u16>> {
            Ok(self.max_retransmits)
        }

        async fn protocol(&self) -> webrtc::error::Result<String> {
            Ok(String::new())
        }

        async fn negotiated(&self) -> webrtc::error::Result<bool> {
            Ok(false)
        }

        fn id(&self) -> RTCDataChannelId {
            0
        }

        async fn ready_state(&self) -> webrtc::error::Result<RTCDataChannelState> {
            Ok(RTCDataChannelState::Open)
        }

        async fn buffered_amount(&self) -> webrtc::error::Result<u64> {
            Ok(self
                .state
                .lock()
                .map(|state| state.buffered_amount)
                .unwrap_or_default())
        }

        async fn buffered_amount_high_threshold(&self) -> webrtc::error::Result<u32> {
            Ok(self.high_threshold)
        }

        async fn set_buffered_amount_high_threshold(
            &self,
            _threshold: u32,
        ) -> webrtc::error::Result<()> {
            Ok(())
        }

        async fn buffered_amount_low_threshold(&self) -> webrtc::error::Result<u32> {
            Ok(0)
        }

        async fn set_buffered_amount_low_threshold(
            &self,
            _threshold: u32,
        ) -> webrtc::error::Result<()> {
            Ok(())
        }

        async fn send(&self, _data: BytesMut) -> webrtc::error::Result<()> {
            Ok(())
        }

        async fn send_with_timeout(
            &self,
            _data: BytesMut,
            _timeout: Duration,
        ) -> webrtc::error::Result<()> {
            let mut state = self
                .state
                .lock()
                .map_err(|_| webrtc::error::Error::ErrUnknownType)?;
            state.calls = state.calls.saturating_add(1);
            match state.outcomes.pop_front().unwrap_or(SendOutcome::Ok(0)) {
                SendOutcome::Ok(_bytes) => Ok(()),
                SendOutcome::Timeout => Err(webrtc::error::Error::ErrTimeout),
            }
        }

        async fn send_text(&self, _text: &str) -> webrtc::error::Result<()> {
            Ok(())
        }

        async fn detach_with_deadline(
            &self,
        ) -> webrtc::error::Result<Arc<dyn DetachedDataChannel>> {
            Err(webrtc::error::Error::ErrDetachNotEnabled)
        }

        async fn poll(&self) -> Option<DataChannelEvent> {
            None
        }

        async fn close(&self) -> webrtc::error::Result<()> {
            Ok(())
        }
    }

    // Upstream reference: livekit/pkg/sfu/datachannel/bitrate_test.go::TestBitrateCalculator
    #[test]
    fn send_rate_samples_matches_bitrate_calculator_contract() {
        let mut samples = SendRateSamples::default();
        let t0 = Instant::now();

        samples.add_sample(t0, 100, 0);
        // buffered amount grows by 100; these bytes are not delivered yet.
        samples.add_sample(t0 + Duration::from_millis(50), 100, 100);
        assert_eq!(samples.bitrate_bps(t0 + Duration::from_millis(50)), None);

        // 100 bytes written + 50 bytes drained from buffered amount.
        samples.add_sample(t0 + Duration::from_secs(1), 100, 50);
        assert_eq!(samples.bitrate_bps(t0 + Duration::from_secs(1)), Some(2000));

        // After a long silence, old samples expire; next sample includes 50 buffered bytes drained.
        let t1 = t0 + (RELIABLE_SLOW_READER_BITRATE_WINDOW * 2);
        samples.add_sample(t1, 100, 0);
        assert_eq!(samples.bitrate_bps(t1 + Duration::from_secs(1)), Some(1200));
    }

    #[test]
    fn send_rate_samples_starts_at_first_post_open_sample() {
        let t0 = Instant::now();
        let mut samples = SendRateSamples::default();
        let first_post_open_write = t0 + Duration::from_secs(1);

        samples.add_sample(first_post_open_write, 100, 0);
        samples.add_sample(
            first_post_open_write + RELIABLE_SLOW_READER_MIN_BITRATE_SAMPLE_WINDOW,
            300,
            0,
        );

        assert_eq!(
            samples.bitrate_bps(
                first_post_open_write + RELIABLE_SLOW_READER_MIN_BITRATE_SAMPLE_WINDOW
            ),
            Some(32_000),
            "pre-open idle time must not dilute the reliable writer bitrate"
        );
    }

    #[test]
    fn send_rate_samples_stays_above_threshold_for_bursty_above_threshold_pattern() {
        const THRESHOLD_BPS: u32 = 21_024;
        let mut samples = SendRateSamples::default();
        let t0 = Instant::now();

        // Establish buffered baseline before applying burst/pause pattern.
        samples.add_sample(t0, 0, 20_000);

        for tick in 1..=80 {
            let now = t0 + Duration::from_millis(25 * tick as u64);
            // Simulate 20% timeout pauses mixed with successful writes.
            let bytes = if tick % 5 == 0 { 0 } else { 160 };
            samples.add_sample(now, bytes, 20_000);

            if let Some(bitrate) = samples.bitrate_bps(now) {
                assert!(
                    bitrate >= THRESHOLD_BPS,
                    "bursty above-threshold pattern should stay above threshold, got {bitrate}"
                );
            }
        }
    }

    #[tokio::test]
    async fn send_bytes_reliable_retries_then_drops_slow_reader() {
        let mock = Arc::new(MockWebRtcDataChannel {
            state: Mutex::new(MockDataChannelState {
                buffered_amount: 0,
                outcomes: VecDeque::from(vec![
                    SendOutcome::Ok(2000),
                    SendOutcome::Timeout,
                    SendOutcome::Ok(10),
                    SendOutcome::Timeout,
                ]),
                calls: 0,
            }),
            high_threshold: 1_000,
            max_retransmits: None,
        });
        let dc = DataChannel::new(mock.clone());

        dc.send_bytes(&vec![0; 2000])
            .await
            .expect("initial reliable write should succeed");

        dc.send_bytes(&[0; 10])
            .await
            .expect("timeout should retry and then succeed when bitrate is healthy");

        tokio::time::sleep(Duration::from_millis(120)).await;
        mock.set_buffered_amount(5_000);
        dc.set_slow_reader_bitrate_threshold_bps(200_000);

        let err = dc.send_bytes(&vec![0; 1000]).await.expect_err(
            "slow reader should drop write when measured bitrate falls below threshold",
        );
        let io_error = err
            .downcast_ref::<std::io::Error>()
            .expect("slow reader drop should map to io::Error");
        assert_eq!(io_error.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(io_error.to_string(), "data dropped by slow reader");

        let calls_after_slow_reader_drop = mock.call_count();
        dc.send_bytes(&vec![0; 1000])
            .await
            .expect("the next reliable packet should receive a fresh write attempt");
        assert_eq!(
            mock.call_count(),
            calls_after_slow_reader_drop + 1,
            "a previous slow-reader drop must not latch future packets into pre-write dropping"
        );
    }

    #[tokio::test]
    async fn send_bytes_reliable_zero_threshold_does_not_retry() {
        let mock = Arc::new(MockWebRtcDataChannel {
            state: Mutex::new(MockDataChannelState {
                buffered_amount: 0,
                outcomes: VecDeque::from(vec![SendOutcome::Timeout]),
                calls: 0,
            }),
            high_threshold: 1_000,
            max_retransmits: None,
        });
        let dc = DataChannel::new(mock.clone());
        dc.set_slow_reader_bitrate_threshold_bps(0);

        let err = dc
            .send_bytes(&vec![0; 1000])
            .await
            .expect_err("zero threshold should disable retry path and return timeout");
        if let Some(io_error) = err.downcast_ref::<std::io::Error>() {
            assert_ne!(io_error.kind(), std::io::ErrorKind::WouldBlock);
        }
        assert_eq!(mock.call_count(), 1);
    }

    #[tokio::test]
    async fn send_bytes_unreliable_drops_when_buffered_amount_is_high() {
        let mock = Arc::new(MockWebRtcDataChannel {
            state: Mutex::new(MockDataChannelState {
                buffered_amount: 0,
                outcomes: VecDeque::from(vec![
                    SendOutcome::Ok(128),
                    SendOutcome::Ok(128),
                    SendOutcome::Ok(128),
                ]),
                calls: 0,
            }),
            high_threshold: 8_192,
            max_retransmits: Some(0),
        });
        let dc = DataChannel::new(mock.clone());

        dc.send_bytes(&[0; 128])
            .await
            .expect("first lossy write should succeed");
        tokio::time::sleep(Duration::from_millis(60)).await;
        dc.send_bytes(&[0; 128])
            .await
            .expect("second lossy write should succeed");
        tokio::time::sleep(Duration::from_millis(60)).await;
        dc.send_bytes(&[0; 128])
            .await
            .expect("third lossy write should succeed");

        mock.set_buffered_amount(4_096);

        let err = dc
            .send_bytes(&vec![0; 4096])
            .await
            .expect_err("unreliable writer should drop when buffered amount is too high");
        let io_error = err
            .downcast_ref::<std::io::Error>()
            .expect("high buffered amount drop should map to io::Error");
        assert_eq!(io_error.kind(), std::io::ErrorKind::WouldBlock);
        assert_eq!(
            io_error.to_string(),
            "data dropped due to high buffered amount"
        );
    }
}
