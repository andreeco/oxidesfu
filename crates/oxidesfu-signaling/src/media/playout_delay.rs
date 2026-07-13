const PLAYOUT_DELAY_EXTENSION_SIZE: usize = 3;

pub(crate) const PLAYOUT_DELAY_URI: &str =
    "http://www.webrtc.org/experiments/rtp-hdrext/playout-delay";
pub(crate) const MAX_PLAYOUT_DELAY_DEFAULT_MS: u16 = 10_000;
pub(crate) const PLAYOUT_DELAY_MAX_VALUE_MS: u16 = 10 * ((1 << 12) - 1);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PlayOutDelayError {
    Overflow,
    BufferTooSmall,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PlayOutDelay {
    pub(crate) min_ms: u16,
    pub(crate) max_ms: u16,
}

impl PlayOutDelay {
    pub(crate) fn from_value(min_ms: u16, max_ms: u16) -> Self {
        Self {
            min_ms: min_ms.min(PLAYOUT_DELAY_MAX_VALUE_MS),
            max_ms: max_ms.min(PLAYOUT_DELAY_MAX_VALUE_MS),
        }
    }

    pub(crate) fn marshal(self) -> Result<[u8; PLAYOUT_DELAY_EXTENSION_SIZE], PlayOutDelayError> {
        let min_units = self.min_ms / 10;
        let max_units = self.max_ms / 10;
        if min_units >= (1 << 12) || max_units >= (1 << 12) {
            return Err(PlayOutDelayError::Overflow);
        }

        Ok([
            (min_units >> 4) as u8,
            ((min_units << 4) as u8) | ((max_units >> 8) as u8),
            max_units as u8,
        ])
    }

    pub(crate) fn unmarshal(raw_data: &[u8]) -> Result<Self, PlayOutDelayError> {
        if raw_data.len() < PLAYOUT_DELAY_EXTENSION_SIZE {
            return Err(PlayOutDelayError::BufferTooSmall);
        }

        let min_units = u16::from_be_bytes([raw_data[0], raw_data[1]]) >> 4;
        let max_units = u16::from_be_bytes([raw_data[1], raw_data[2]]) & 0x0fff;

        Ok(Self {
            min_ms: min_units * 10,
            max_ms: max_units * 10,
        })
    }
}

#[cfg(test)]
mod tests {
    use super::{
        MAX_PLAYOUT_DELAY_DEFAULT_MS, PLAYOUT_DELAY_EXTENSION_SIZE, PLAYOUT_DELAY_MAX_VALUE_MS,
        PLAYOUT_DELAY_URI, PlayOutDelay, PlayOutDelayError,
    };

    #[test]
    fn playout_delay_marshal_unmarshal_matches_upstream_contract() {
        assert_eq!(
            PLAYOUT_DELAY_URI,
            "http://www.webrtc.org/experiments/rtp-hdrext/playout-delay"
        );
        assert_eq!(MAX_PLAYOUT_DELAY_DEFAULT_MS, 10_000);

        let expected = PlayOutDelay {
            min_ms: 100,
            max_ms: 200,
        };
        let encoded = expected
            .marshal()
            .expect("valid playout delay should marshal");
        assert_eq!(encoded.len(), PLAYOUT_DELAY_EXTENSION_SIZE);

        let decoded =
            PlayOutDelay::unmarshal(&encoded).expect("marshaled playout delay should unmarshal");
        assert_eq!(decoded, expected);
    }

    #[test]
    fn playout_delay_marshal_rejects_overflow() {
        let overflow = PlayOutDelay {
            min_ms: 100,
            max_ms: (1 << 12) * 10,
        };

        let result = overflow.marshal();
        assert_eq!(result, Err(PlayOutDelayError::Overflow));
    }

    #[test]
    fn playout_delay_unmarshal_rejects_too_small_buffer() {
        let result = PlayOutDelay::unmarshal(&[0x00, 0x00]);
        assert_eq!(result, Err(PlayOutDelayError::BufferTooSmall));
    }

    #[test]
    fn playout_delay_from_value_clamps_to_max_representable() {
        let clamped = PlayOutDelay::from_value((1 << 12) * 10, (1 << 12) * 10 + 10);

        assert_eq!(clamped.min_ms, PLAYOUT_DELAY_MAX_VALUE_MS);
        assert_eq!(clamped.max_ms, PLAYOUT_DELAY_MAX_VALUE_MS);
        clamped
            .marshal()
            .expect("clamped playout delay should still marshal");
    }

    #[test]
    fn playout_delay_roundtrips_at_max_representable_value() {
        let expected = PlayOutDelay {
            min_ms: 100,
            max_ms: PLAYOUT_DELAY_MAX_VALUE_MS,
        };

        let encoded = expected.marshal().expect("max value should marshal");
        let decoded = PlayOutDelay::unmarshal(&encoded).expect("max value should unmarshal");

        assert_eq!(decoded, expected);
    }
}
