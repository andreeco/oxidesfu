use std::sync::Mutex;
use std::time::Instant;

use super::playout_delay::{
    MAX_PLAYOUT_DELAY_DEFAULT_MS, PLAYOUT_DELAY_MAX_VALUE_MS, PlayOutDelay,
};

const JITTER_MULTI_TO_DELAY: u32 = 10;
const MAX_DELAY_CHANGE_PER_SEC: u32 = 80;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum PlayoutDelayState {
    Changed,
    Sending,
    Acked,
}

#[derive(Debug, Clone, Copy)]
struct PlayoutDelayControllerInner {
    state: PlayoutDelayState,
    min_delay_ms: u32,
    max_delay_ms: u32,
    current_delay_ms: u32,
    sending_at_seq: u16,
    sending_at_time: Instant,
}

#[derive(Debug)]
#[allow(dead_code)]
pub(crate) struct PlayoutDelayController {
    inner: Mutex<PlayoutDelayControllerInner>,
    ext_bytes: Mutex<Vec<u8>>,
}

#[allow(dead_code)]
impl PlayoutDelayController {
    pub(crate) fn new(min_delay_ms: u32, mut max_delay_ms: u32) -> Self {
        if max_delay_ms == 0 && min_delay_ms > 0 {
            max_delay_ms = MAX_PLAYOUT_DELAY_DEFAULT_MS as u32;
        }
        if max_delay_ms > PLAYOUT_DELAY_MAX_VALUE_MS as u32 {
            max_delay_ms = PLAYOUT_DELAY_MAX_VALUE_MS as u32;
        }

        let controller = Self {
            inner: Mutex::new(PlayoutDelayControllerInner {
                state: PlayoutDelayState::Changed,
                min_delay_ms,
                max_delay_ms,
                current_delay_ms: min_delay_ms,
                sending_at_seq: 0,
                sending_at_time: Instant::now(),
            }),
            ext_bytes: Mutex::new(Vec::new()),
        };
        controller.refresh_extension_bytes();
        controller
    }

    pub(crate) fn set_jitter(&self, jitter_ms: u32) {
        let mut inner = self
            .inner
            .lock()
            .expect("playout delay lock should be available");

        let mut target_delay = jitter_ms.saturating_mul(JITTER_MULTI_TO_DELAY);
        let elapsed = inner.sending_at_time.elapsed();
        let mut delay_change_limit =
            (MAX_DELAY_CHANGE_PER_SEC as f64 * elapsed.as_secs_f64()).floor() as u32;
        if delay_change_limit > MAX_DELAY_CHANGE_PER_SEC {
            delay_change_limit = MAX_DELAY_CHANGE_PER_SEC;
        }

        if target_delay > inner.current_delay_ms.saturating_add(delay_change_limit) {
            target_delay = inner.current_delay_ms.saturating_add(delay_change_limit);
        } else if inner.current_delay_ms > target_delay.saturating_add(delay_change_limit) {
            target_delay = inner.current_delay_ms.saturating_sub(delay_change_limit);
        }

        if target_delay < inner.min_delay_ms {
            target_delay = inner.min_delay_ms;
        }
        if target_delay > inner.max_delay_ms {
            target_delay = inner.max_delay_ms;
        }

        if target_delay == inner.current_delay_ms {
            return;
        }

        inner.current_delay_ms = target_delay;
        drop(inner);
        self.refresh_extension_bytes();
    }

    pub(crate) fn on_seq_acked(&self, seq: u16) {
        let mut inner = self
            .inner
            .lock()
            .expect("playout delay lock should be available");
        if inner.state == PlayoutDelayState::Sending
            && seq.wrapping_sub(inner.sending_at_seq) < 0x8000
        {
            inner.state = PlayoutDelayState::Acked;
        }
    }

    pub(crate) fn get_delay_extension(&self, seq: u16) -> Option<Vec<u8>> {
        let mut inner = self
            .inner
            .lock()
            .expect("playout delay lock should be available");
        match inner.state {
            PlayoutDelayState::Changed => {
                inner.state = PlayoutDelayState::Sending;
                inner.sending_at_seq = seq;
                inner.sending_at_time = Instant::now();
                Some(
                    self.ext_bytes
                        .lock()
                        .expect("extension bytes lock should be available")
                        .clone(),
                )
            }
            PlayoutDelayState::Sending => Some(
                self.ext_bytes
                    .lock()
                    .expect("extension bytes lock should be available")
                    .clone(),
            ),
            PlayoutDelayState::Acked => None,
        }
    }

    fn refresh_extension_bytes(&self) {
        let mut inner = self
            .inner
            .lock()
            .expect("playout delay lock should be available");
        let delay =
            PlayOutDelay::from_value(inner.current_delay_ms as u16, inner.max_delay_ms as u16);
        let bytes = delay
            .marshal()
            .expect("playout-delay extension values should always fit after clamping");
        let mut ext = self
            .ext_bytes
            .lock()
            .expect("extension bytes lock should be available");
        ext.clear();
        ext.extend_from_slice(&bytes);
        inner.state = PlayoutDelayState::Changed;
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::PlayoutDelayController;
    use crate::media::playout_delay::PlayOutDelay;

    fn playout_delay_equal(data: &[u8], min: u16, max: u16) {
        let decoded = PlayOutDelay::unmarshal(data).expect("delay extension should unmarshal");
        assert_eq!(decoded.min_ms, min);
        assert_eq!(decoded.max_ms, max);
    }

    #[test]
    fn playout_delay_controller_matches_upstream_contract() {
        let controller = PlayoutDelayController::new(100, 120);

        let ext = controller
            .get_delay_extension(100)
            .expect("initial extension should be present");
        playout_delay_equal(&ext, 100, 120);

        let ext = controller
            .get_delay_extension(105)
            .expect("should still send extension while waiting for ack");
        playout_delay_equal(&ext, 100, 120);

        controller.on_seq_acked(65534);
        let ext = controller
            .get_delay_extension(105)
            .expect("ack before sending sequence should not stop extension emission");
        playout_delay_equal(&ext, 100, 120);

        controller.on_seq_acked(90);
        let ext = controller
            .get_delay_extension(105)
            .expect("older ack should not stop extension emission");
        playout_delay_equal(&ext, 100, 120);

        controller.on_seq_acked(103);
        assert!(
            controller.get_delay_extension(106).is_none(),
            "acked sending sequence should stop extension emission"
        );

        controller.set_jitter(0);
        assert!(
            controller.get_delay_extension(107).is_none(),
            "no delay change should not emit a new extension"
        );

        std::thread::sleep(Duration::from_millis(200));
        controller.set_jitter(50);
        let ext = controller
            .get_delay_extension(108)
            .expect("delay increase should emit a new extension");
        let decoded = PlayOutDelay::unmarshal(&ext).expect("updated extension should unmarshal");
        assert!(decoded.min_ms > 100);

        std::thread::sleep(Duration::from_millis(200));
        controller.set_jitter(10_000);
        let ext = controller
            .get_delay_extension(109)
            .expect("delay update should emit extension capped by max delay");
        playout_delay_equal(&ext, 120, 120);
    }

    #[test]
    fn playout_delay_controller_max_delay_defaults_and_clamps() {
        let defaulted = PlayoutDelayController::new(100, 0);
        let ext = defaulted
            .get_delay_extension(1)
            .expect("defaulted max-delay controller should emit extension");
        playout_delay_equal(&ext, 100, 10_000);

        let clamped = PlayoutDelayController::new(100, 100_000);
        let ext = clamped
            .get_delay_extension(1)
            .expect("clamped max-delay controller should emit extension");
        let decoded = PlayOutDelay::unmarshal(&ext).expect("clamped extension should unmarshal");
        assert_eq!(decoded.max_ms, 40_950);
    }

    #[test]
    fn playout_delay_controller_zero_max_with_zero_min_stays_zero() {
        let controller = PlayoutDelayController::new(0, 0);
        let ext = controller
            .get_delay_extension(1)
            .expect("zero-delay controller should emit extension");
        playout_delay_equal(&ext, 0, 0);

        std::thread::sleep(Duration::from_millis(120));
        controller.set_jitter(10);
        let updated = controller
            .get_delay_extension(2)
            .expect("jitter update should emit extension");
        let decoded =
            PlayOutDelay::unmarshal(&updated).expect("updated extension should unmarshal");
        assert_eq!(decoded.max_ms, 0);
        assert_eq!(decoded.min_ms, 0);
    }
}
