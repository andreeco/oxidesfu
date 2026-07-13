#[cfg(test)]
mod tests {
    fn shuffle_with_seed(values: &mut [u64], mut seed: u64) {
        if values.len() <= 1 {
            return;
        }
        for i in (1..values.len()).rev() {
            seed = seed.wrapping_mul(6364136223846793005).wrapping_add(1);
            let j = (seed as usize) % (i + 1);
            values.swap(i, j);
        }
    }

    #[derive(Debug, Clone, Copy, Default)]
    struct DependencyDescriptorLite {
        first_packet_in_frame: bool,
        last_packet_in_frame: bool,
    }

    #[derive(Debug, Clone)]
    struct PacketHistory {
        base: u64,
        last: u64,
        bits: Vec<u64>,
        packet_count: usize,
        inited: bool,
    }

    impl PacketHistory {
        fn new(packet_count: usize) -> Self {
            let aligned_packet_count = packet_count.div_ceil(64) * 64;
            Self {
                base: 0,
                last: 0,
                bits: vec![0; aligned_packet_count / 64],
                packet_count: aligned_packet_count,
                inited: false,
            }
        }

        fn add_packet(&mut self, ext_seq: u64) {
            if !self.inited {
                self.inited = true;
                self.base = ext_seq.saturating_sub(100);
                self.last = ext_seq;
                self.set(ext_seq, true);
                return;
            }

            if ext_seq <= self.base {
                return;
            }

            if ext_seq <= self.last {
                if self.last - ext_seq < self.packet_count as u64 {
                    self.set(ext_seq, true);
                }
                return;
            }

            for seq in (self.last + 1)..ext_seq {
                self.set(seq, false);
            }
            self.set(ext_seq, true);
            self.last = ext_seq;
        }

        fn packets_consecutive(&self, start: u64, end: u64) -> bool {
            if start > end || end - start >= self.packet_count as u64 {
                return false;
            }

            let (start_index, start_offset) = self.get_pos(start);
            let (end_index, end_offset) = self.get_pos(end);

            if start_index == end_index && end - start <= 64 {
                let width = end_offset - start_offset + 1;
                let test_bits = if width >= 64 {
                    u64::MAX
                } else {
                    ((1u64 << width) - 1) << start_offset
                };
                return (self.bits[start_index] & test_bits) == test_bits;
            }

            let expected = if start_offset == 0 {
                0u64
            } else {
                1u64 << (64 - start_offset)
            };
            if (self.bits[start_index] >> start_offset).wrapping_add(1) != expected {
                return false;
            }

            let mut idx = start_index + 1;
            while idx != end_index {
                if idx == self.bits.len() {
                    idx = 0;
                    if idx == end_index {
                        break;
                    }
                }
                if self.bits[idx].wrapping_add(1) != 0 {
                    return false;
                }
                idx += 1;
            }

            let test_bits = if end_offset + 1 >= 64 {
                u64::MAX
            } else {
                (1u64 << (end_offset + 1)) - 1
            };
            (self.bits[end_index] & test_bits) == test_bits
        }

        fn set(&mut self, seq: u64, received: bool) {
            let (index, offset) = self.get_pos(seq);
            if received {
                self.bits[index] |= 1 << offset;
            } else {
                self.bits[index] &= !(1 << offset);
            }
        }

        fn get_pos(&self, seq: u64) -> (usize, usize) {
            let idx = (seq - self.base) % self.packet_count as u64;
            ((idx >> 6) as usize, (idx % 64) as usize)
        }
    }

    #[derive(Debug, Clone)]
    struct FrameEntity {
        start_seq: Option<u64>,
        end_seq: Option<u64>,
        integrity: bool,
    }

    impl FrameEntity {
        fn reset(&mut self) {
            self.start_seq = None;
            self.end_seq = None;
            self.integrity = false;
        }

        fn add_packet(
            &mut self,
            ext_seq: u64,
            dd: DependencyDescriptorLite,
            packet_history: &PacketHistory,
        ) {
            if self.integrity {
                return;
            }

            if self.start_seq.is_none() && dd.first_packet_in_frame {
                self.start_seq = Some(ext_seq);
            }
            if self.end_seq.is_none() && dd.last_packet_in_frame {
                self.end_seq = Some(ext_seq);
            }

            if let (Some(start), Some(end)) = (self.start_seq, self.end_seq)
                && packet_history.packets_consecutive(start, end)
            {
                self.integrity = true;
            }
        }
    }

    #[derive(Debug, Clone)]
    struct FrameIntegrityCheckerLite {
        frame_count: usize,
        frames: Vec<FrameEntity>,
        base: u64,
        last: u64,
        packet_history: PacketHistory,
        inited: bool,
    }

    impl FrameIntegrityCheckerLite {
        fn new(frame_count: usize, packet_count: usize) -> Self {
            Self {
                frame_count,
                frames: vec![
                    FrameEntity {
                        start_seq: None,
                        end_seq: None,
                        integrity: false
                    };
                    frame_count
                ],
                base: 0,
                last: 0,
                packet_history: PacketHistory::new(packet_count),
                inited: false,
            }
        }

        fn add_packet(&mut self, ext_seq: u64, ext_frame_num: u64, dd: DependencyDescriptorLite) {
            self.packet_history.add_packet(ext_seq);

            if !self.inited {
                self.inited = true;
                self.base = ext_frame_num;
                self.last = ext_frame_num;
            }

            if ext_frame_num < self.base {
                return;
            }

            if ext_frame_num <= self.last {
                if self.last - ext_frame_num >= self.frame_count as u64 {
                    return;
                }
                let idx = ((ext_frame_num - self.base) % self.frame_count as u64) as usize;
                self.frames[idx].add_packet(ext_seq, dd, &self.packet_history);
                return;
            }

            for frame in (self.last + 1)..=ext_frame_num {
                let idx = ((frame - self.base) % self.frame_count as u64) as usize;
                self.frames[idx].reset();
            }
            let idx = ((ext_frame_num - self.base) % self.frame_count as u64) as usize;
            self.frames[idx].add_packet(ext_seq, dd, &self.packet_history);
            self.last = ext_frame_num;
        }

        fn frame_integrity(&self, ext_frame_num: u64) -> bool {
            if !self.inited
                || ext_frame_num < self.base
                || ext_frame_num > self.last
                || self.last - ext_frame_num >= self.frame_count as u64
            {
                return false;
            }
            let idx = ((ext_frame_num - self.base) % self.frame_count as u64) as usize;
            self.frames[idx].integrity
        }
    }

    // Upstream: livekit/pkg/sfu/buffer/frameintegrity_test.go::TestFrameIntegrityChecker
    #[test]
    fn frame_integrity_checker_matches_upstream_contract() {
        let mut checker = FrameIntegrityCheckerLite::new(100, 1000);

        checker.add_packet(10, 10, DependencyDescriptorLite::default());
        assert!(!checker.frame_integrity(10));
        checker.add_packet(
            9,
            10,
            DependencyDescriptorLite {
                first_packet_in_frame: true,
                last_packet_in_frame: false,
            },
        );
        assert!(!checker.frame_integrity(10));
        checker.add_packet(
            11,
            10,
            DependencyDescriptorLite {
                first_packet_in_frame: false,
                last_packet_in_frame: true,
            },
        );
        assert!(checker.frame_integrity(10));

        checker.add_packet(
            100,
            100,
            DependencyDescriptorLite {
                first_packet_in_frame: true,
                last_packet_in_frame: true,
            },
        );
        assert!(checker.frame_integrity(100));
        assert!(!checker.frame_integrity(101));
        assert!(!checker.frame_integrity(99));

        checker.add_packet(
            99,
            99,
            DependencyDescriptorLite {
                first_packet_in_frame: true,
                last_packet_in_frame: true,
            },
        );

        checker.add_packet(2001, 2001, DependencyDescriptorLite::default());
        assert!(!checker.frame_integrity(2001));
        assert!(!checker.frame_integrity(1999));
        assert!(!checker.frame_integrity(100));
        assert!(!checker.frame_integrity(1900));

        checker.add_packet(
            2000,
            2001,
            DependencyDescriptorLite {
                first_packet_in_frame: true,
                last_packet_in_frame: false,
            },
        );
        assert!(!checker.frame_integrity(2001));
        checker.add_packet(
            2002,
            2001,
            DependencyDescriptorLite {
                first_packet_in_frame: false,
                last_packet_in_frame: true,
            },
        );
        assert!(checker.frame_integrity(2001));

        checker.add_packet(2001, 2001, DependencyDescriptorLite::default());
        assert!(checker.frame_integrity(2001));

        checker.add_packet(
            900,
            1900,
            DependencyDescriptorLite {
                first_packet_in_frame: true,
                last_packet_in_frame: true,
            },
        );
        assert!(!checker.frame_integrity(1900));

        for frame in 2002u64..2102u64 {
            let first = 3000 + (frame - 2002) * 1000;
            let last = 3999 + (frame - 2002) * 1000;
            let mut seqs: Vec<u64> = (first..=last).collect();
            assert!(!checker.frame_integrity(frame));

            shuffle_with_seed(&mut seqs, frame);
            for (index, seq) in seqs.iter().enumerate() {
                checker.add_packet(
                    *seq,
                    frame,
                    DependencyDescriptorLite {
                        first_packet_in_frame: *seq == first,
                        last_packet_in_frame: *seq == last,
                    },
                );
                assert_eq!(index == seqs.len() - 1, checker.frame_integrity(frame));
            }
            assert!(checker.frame_integrity(frame));
        }
    }
}
