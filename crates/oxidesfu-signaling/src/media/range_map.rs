use std::cmp::max;

const MIN_RANGES: usize = 1;
const HALF_RANGE_U32: u32 = 1 << 31;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RangeMapError {
    ReversedOrder,
    KeyNotFound,
    KeyTooOld,
    KeyExcluded,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeVal {
    start: u32,
    end: u32,
    value: u32,
}

#[derive(Debug, Clone, PartialEq, Eq)]
#[allow(dead_code)]
pub(crate) struct RangeMapU32 {
    size: usize,
    ranges: Vec<RangeVal>,
}

#[allow(dead_code)]
impl RangeMapU32 {
    pub(crate) fn new(size: usize) -> Self {
        let mut range_map = Self {
            size: max(size, MIN_RANGES),
            ranges: Vec::new(),
        };
        range_map.init_ranges(0, 0);
        range_map
    }

    pub(crate) fn clear_and_reset_value(&mut self, start: u32, value: u32) {
        self.init_ranges(start, value);
    }

    pub(crate) fn dec_value(&mut self, end: u32, dec: u32) {
        let last_index = self.ranges.len() - 1;
        let last_range = &mut self.ranges[last_index];
        if last_range.start > end {
            // Modify existing open-range value when `end` is before open-range start.
            last_range.value = last_range.value.saturating_sub(dec);
            return;
        }

        // Close open range and start a new open range with decremented value.
        last_range.end = end;
        let next_start = end.wrapping_add(1);
        let next_value = last_range.value.saturating_sub(dec);
        self.ranges.push(RangeVal {
            start: next_start,
            end: 0,
            value: next_value,
        });
        self.prune();
    }

    pub(crate) fn exclude_range(
        &mut self,
        start_inclusive: u32,
        end_exclusive: u32,
    ) -> Result<(), RangeMapError> {
        let width = end_exclusive.wrapping_sub(start_inclusive);
        if end_exclusive == start_inclusive || width > HALF_RANGE_U32 {
            return Err(RangeMapError::ReversedOrder);
        }

        let last_index = self.ranges.len() - 1;
        let last_range = &mut self.ranges[last_index];
        if last_range.start > start_inclusive {
            return Err(RangeMapError::ReversedOrder);
        }

        let new_value = last_range.value.saturating_add(width);

        if last_range.start == start_inclusive {
            // Extend open range directly.
            last_range.start = end_exclusive;
            last_range.value = new_value;
            return Ok(());
        }

        // Close previous range and append new open range.
        last_range.end = start_inclusive.wrapping_sub(1);
        self.ranges.push(RangeVal {
            start: end_exclusive,
            end: 0,
            value: new_value,
        });

        self.prune();
        Ok(())
    }

    pub(crate) fn get_value(&self, key: u32) -> Result<u32, RangeMapError> {
        let num_ranges = self.ranges.len();
        if num_ranges != 0 {
            if key >= self.ranges[num_ranges - 1].start {
                // Open range.
                return Ok(self.ranges[num_ranges - 1].value);
            }

            if key < self.ranges[0].start {
                return Err(RangeMapError::KeyTooOld);
            }
        }

        for idx in (0..num_ranges).rev() {
            let range = &self.ranges[idx];
            if idx != num_ranges - 1 {
                // Closed range check.
                if key.wrapping_sub(range.start) < HALF_RANGE_U32
                    && range.end.wrapping_sub(key) < HALF_RANGE_U32
                {
                    return Ok(range.value);
                }
            }

            if idx > 0 {
                let previous = &self.ranges[idx - 1];
                let before_diff = key.wrapping_sub(previous.end);
                let after_diff = range.start.wrapping_sub(key);
                if before_diff > 0
                    && before_diff < HALF_RANGE_U32
                    && after_diff > 0
                    && after_diff < HALF_RANGE_U32
                {
                    return Err(RangeMapError::KeyExcluded);
                }
            }
        }

        Err(RangeMapError::KeyNotFound)
    }

    fn init_ranges(&mut self, start: u32, value: u32) {
        self.ranges = vec![RangeVal {
            start,
            end: 0,
            value,
        }];
    }

    fn prune(&mut self) {
        if self.ranges.len() > self.size + 1 {
            self.ranges = self.ranges[self.ranges.len() - self.size - 1..].to_vec();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{RangeMapError, RangeMapU32, RangeVal};

    #[test]
    fn range_map_u32_matches_upstream_contract() {
        let mut range_map = RangeMapU32::new(2);

        let mut value = range_map
            .get_value(33333)
            .expect("default open range value should be readable");
        assert_eq!(value, 0);

        let mut expected_ranges = vec![RangeVal {
            start: 0,
            end: 0,
            value: 0,
        }];
        assert_eq!(range_map.ranges, expected_ranges);

        range_map
            .exclude_range(10, 11)
            .expect("first exclusion should succeed");

        expected_ranges = vec![
            RangeVal {
                start: 0,
                end: 9,
                value: 0,
            },
            RangeVal {
                start: 11,
                end: 0,
                value: 1,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);

        value = range_map
            .get_value(6)
            .expect("old closed range value should be readable");
        assert_eq!(value, 0);

        value = range_map
            .get_value(11)
            .expect("new open range value should be readable");
        assert_eq!(value, 1);

        assert_eq!(range_map.get_value(10), Err(RangeMapError::KeyExcluded));
        assert_eq!(
            range_map.exclude_range(9, 10),
            Err(RangeMapError::ReversedOrder)
        );
        assert_eq!(
            range_map.exclude_range(12, 11),
            Err(RangeMapError::ReversedOrder)
        );
        assert_eq!(
            range_map.exclude_range(11, 11),
            Err(RangeMapError::ReversedOrder)
        );

        range_map
            .exclude_range(11, 12)
            .expect("adjacent exclusion should extend open range");
        expected_ranges = vec![
            RangeVal {
                start: 0,
                end: 9,
                value: 0,
            },
            RangeVal {
                start: 12,
                end: 0,
                value: 2,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);

        assert_eq!(range_map.get_value(11), Err(RangeMapError::KeyExcluded));
        assert!(range_map.get_value(6).is_ok());

        value = range_map
            .get_value(12)
            .expect("extended open range value should be readable");
        assert_eq!(value, 2);

        range_map
            .exclude_range(12, 22)
            .expect("length-10 adjacent exclusion should succeed");
        expected_ranges = vec![
            RangeVal {
                start: 0,
                end: 9,
                value: 0,
            },
            RangeVal {
                start: 22,
                end: 0,
                value: 12,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);

        assert_eq!(range_map.get_value(15), Err(RangeMapError::KeyExcluded));
        value = range_map
            .get_value(25)
            .expect("open range should reflect cumulative exclusions");
        assert_eq!(value, 12);

        range_map
            .exclude_range(26, 30)
            .expect("disjoint exclusion should close current open range and open a new one");
        expected_ranges = vec![
            RangeVal {
                start: 0,
                end: 9,
                value: 0,
            },
            RangeVal {
                start: 22,
                end: 25,
                value: 12,
            },
            RangeVal {
                start: 30,
                end: 0,
                value: 16,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);

        value = range_map
            .get_value(23)
            .expect("newly closed range should return expected value");
        assert_eq!(value, 12);

        range_map
            .exclude_range(50, 51)
            .expect("further disjoint exclusion should trigger pruning");
        expected_ranges = vec![
            RangeVal {
                start: 22,
                end: 25,
                value: 12,
            },
            RangeVal {
                start: 30,
                end: 49,
                value: 16,
            },
            RangeVal {
                start: 51,
                end: 0,
                value: 17,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);

        assert_eq!(range_map.get_value(50), Err(RangeMapError::KeyExcluded));
        assert_eq!(range_map.get_value(28), Err(RangeMapError::KeyExcluded));
        assert_eq!(range_map.get_value(17), Err(RangeMapError::KeyTooOld));
        assert_eq!(range_map.get_value(5), Err(RangeMapError::KeyTooOld));

        assert_eq!(range_map.get_value(24), Ok(12));
        assert_eq!(range_map.get_value(34), Ok(16));
        assert_eq!(range_map.get_value(49), Ok(16));
        assert_eq!(range_map.get_value(55_555_555), Ok(17));

        range_map.clear_and_reset_value(24, 23);
        expected_ranges = vec![RangeVal {
            start: 24,
            end: 0,
            value: 23,
        }];
        assert_eq!(range_map.ranges, expected_ranges);
        assert_eq!(range_map.get_value(55_555_555), Ok(23));

        range_map.dec_value(34, 12);
        expected_ranges = vec![
            RangeVal {
                start: 24,
                end: 34,
                value: 23,
            },
            RangeVal {
                start: 35,
                end: 0,
                value: 11,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);
        assert_eq!(range_map.get_value(55_555_555), Ok(11));

        range_map
            .exclude_range(40, 45)
            .expect("exclusion should split current open range");
        expected_ranges = vec![
            RangeVal {
                start: 24,
                end: 34,
                value: 23,
            },
            RangeVal {
                start: 35,
                end: 39,
                value: 11,
            },
            RangeVal {
                start: 45,
                end: 0,
                value: 16,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);

        assert_eq!(range_map.get_value(5), Err(RangeMapError::KeyTooOld));
        assert_eq!(range_map.get_value(25), Ok(23));
        assert_eq!(range_map.get_value(35), Ok(11));
        assert_eq!(range_map.get_value(55_555_555), Ok(16));

        range_map.dec_value(66, 6);
        expected_ranges = vec![
            RangeVal {
                start: 35,
                end: 39,
                value: 11,
            },
            RangeVal {
                start: 45,
                end: 66,
                value: 16,
            },
            RangeVal {
                start: 67,
                end: 0,
                value: 10,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);

        assert_eq!(range_map.get_value(25), Err(RangeMapError::KeyTooOld));
        assert_eq!(range_map.get_value(66), Ok(16));
        assert_eq!(range_map.get_value(67), Ok(10));

        range_map.dec_value(66, 6);
        expected_ranges = vec![
            RangeVal {
                start: 35,
                end: 39,
                value: 11,
            },
            RangeVal {
                start: 45,
                end: 66,
                value: 16,
            },
            RangeVal {
                start: 67,
                end: 0,
                value: 4,
            },
        ];
        assert_eq!(range_map.ranges, expected_ranges);
        assert_eq!(range_map.get_value(67), Ok(4));
    }
}
