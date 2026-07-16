#[cfg(test)]
#[allow(clippy::iter_overeager_cloned)]
mod tests {
    use std::array;

    const MAX_RED_COUNT: usize = 2;
    const MTU_SIZE: usize = 1500;
    const MAX_RED_PAYLOAD: usize = 1 << 10;
    const OPUS_PT: u8 = 111;
    const OPUS_RED_PT: u8 = 63;
    const TS_STEP: u32 = 480;

    #[derive(Debug, Clone, PartialEq, Eq)]
    struct RtpPacketLite {
        sequence_number: u16,
        timestamp: u32,
        payload_type: u8,
        payload: Vec<u8>,
    }

    impl RtpPacketLite {
        fn with_payload(
            sequence_number: u16,
            timestamp: u32,
            payload_type: u8,
            payload: Vec<u8>,
        ) -> Self {
            Self {
                sequence_number,
                timestamp,
                payload_type,
                payload,
            }
        }
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    struct RedBlock {
        ts_offset: u32,
        length: usize,
        pt: u8,
        primary: bool,
    }

    #[derive(Debug, Clone, Copy, PartialEq, Eq)]
    enum RedError {
        IncompleteHeader,
        IncompleteBlock,
    }

    struct RedReceiverLite {
        pkt_buff: [Option<RtpPacketLite>; MAX_RED_COUNT],
    }

    impl RedReceiverLite {
        fn new() -> Self {
            Self {
                pkt_buff: array::from_fn(|_| None),
            }
        }

        fn encode_red_for_primary(
            &mut self,
            pkt: &RtpPacketLite,
            red_payload: &mut [u8],
        ) -> Result<usize, RedError> {
            let red_length = self.pkt_buff.len();
            let mut red_pkts = Vec::with_capacity(red_length + 1);

            let mut last_nil = None;
            for i in (0..red_length).rev() {
                if self.pkt_buff[i].is_none() {
                    last_nil = Some(i);
                    break;
                }
            }

            let start = last_nil.map_or(0usize, |i| i + 1);
            for prev in self.pkt_buff[start..].iter().flatten() {
                let sn_diff = pkt.sequence_number.wrapping_sub(prev.sequence_number);
                let ts_diff = pkt.timestamp.wrapping_sub(prev.timestamp);
                if pkt.sequence_number == prev.sequence_number
                    || sn_diff > red_length as u16
                    || ts_diff >= (1 << 14)
                {
                    continue;
                }
                red_pkts.push(prev.clone());
            }

            for i in (0..red_length).rev() {
                let should_insert = match &self.pkt_buff[i] {
                    None => true,
                    Some(existing) => {
                        pkt.sequence_number.wrapping_sub(existing.sequence_number) < (1 << 15)
                    }
                };
                if should_insert {
                    for j in 0..i {
                        self.pkt_buff[j] = self.pkt_buff[j + 1].clone();
                    }
                    self.pkt_buff[i] = Some(pkt.clone());
                    break;
                }
            }

            encode_red_for_primary_blocks(&red_pkts, pkt, red_payload)
        }
    }

    struct RedPrimaryReceiverLite {
        red_pt: u8,
        first_pkt_received: bool,
        last_seq: u16,
        // history for [last_seq-8, last_seq-1], bit=1 means packet seen
        pkt_history: u8,
    }

    impl RedPrimaryReceiverLite {
        fn new(red_pt: u8) -> Self {
            Self {
                red_pt,
                first_pkt_received: false,
                last_seq: 0,
                pkt_history: 0,
            }
        }

        fn forward_rtp(&mut self, pkt: &RtpPacketLite) -> Result<Vec<RtpPacketLite>, RedError> {
            if pkt.payload_type != self.red_pt {
                return Ok(vec![pkt.clone()]);
            }

            self.get_send_pkts_from_red(pkt)
        }

        fn get_send_pkts_from_red(
            &mut self,
            rtp: &RtpPacketLite,
        ) -> Result<Vec<RtpPacketLite>, RedError> {
            let mut need_recover = false;

            if !self.first_pkt_received {
                self.last_seq = rtp.sequence_number;
                self.pkt_history = 0;
                self.first_pkt_received = true;
            } else {
                let diff = rtp.sequence_number.wrapping_sub(self.last_seq);
                if diff == 0 {
                    // duplicate
                } else if diff > 0x8000 {
                    // out-of-order
                    if 65535u16.wrapping_sub(diff) < 8 {
                        self.pkt_history |= 1 << (65535u16.wrapping_sub(diff));
                        need_recover = true;
                    }
                } else if diff > 8 {
                    // long jump
                    self.last_seq = rtp.sequence_number;
                    self.pkt_history = 0;
                    need_recover = true;
                } else {
                    self.last_seq = rtp.sequence_number;
                    self.pkt_history = (self.pkt_history << diff) | (1 << (diff - 1));
                    need_recover = true;
                }
            }

            let mut recover_bits = 0u8;
            if need_recover {
                let mut bit_index = self.last_seq.wrapping_sub(rtp.sequence_number);
                for i in 0..MAX_RED_COUNT {
                    if bit_index > 7 {
                        break;
                    }
                    if self.pkt_history & (1 << bit_index) == 0 {
                        recover_bits |= 1 << i;
                    }
                    bit_index = bit_index.wrapping_add(1);
                }
            }

            extract_pkts_from_red(rtp, recover_bits)
        }
    }

    fn encode_red_for_primary_blocks(
        red_pkts: &[RtpPacketLite],
        primary: &RtpPacketLite,
        red_payload: &mut [u8],
    ) -> Result<usize, RedError> {
        let mut payload_size = primary.payload.len() + 1;
        for p in red_pkts {
            payload_size += p.payload.len() + 4;
        }

        let mut red_pkts = red_pkts.to_vec();
        if payload_size > red_payload.len() {
            red_pkts.clear();
        }

        let mut index = 0usize;
        for p in &red_pkts {
            let mut header = (0x80u32 | OPUS_PT as u32) & 0xFF;
            header <<= 14;
            header |= primary.timestamp.wrapping_sub(p.timestamp) & 0x3FFF;
            header <<= 10;
            header |= (p.payload.len() as u32) & 0x3FF;

            if red_payload.len().saturating_sub(index) < 4 {
                return Err(RedError::IncompleteBlock);
            }
            red_payload[index] = ((header >> 24) & 0xFF) as u8;
            red_payload[index + 1] = ((header >> 16) & 0xFF) as u8;
            red_payload[index + 2] = ((header >> 8) & 0xFF) as u8;
            red_payload[index + 3] = (header & 0xFF) as u8;
            index += 4;
        }

        if red_payload.len().saturating_sub(index) < 1 {
            return Err(RedError::IncompleteBlock);
        }
        red_payload[index] = OPUS_PT;
        index += 1;

        for p in red_pkts.iter().chain(std::iter::once(primary)) {
            if red_payload.len().saturating_sub(index) < p.payload.len() {
                return Err(RedError::IncompleteBlock);
            }
            red_payload[index..index + p.payload.len()].copy_from_slice(&p.payload);
            index += p.payload.len();
        }

        Ok(index)
    }

    fn extract_pkts_from_red(
        red_pkt: &RtpPacketLite,
        recover_bits: u8,
    ) -> Result<Vec<RtpPacketLite>, RedError> {
        let mut payload = red_pkt.payload.as_slice();
        let mut blocks = Vec::<RedBlock>::new();
        let mut block_length = 0usize;

        loop {
            if payload.is_empty() {
                return Err(RedError::IncompleteHeader);
            }

            if payload[0] & 0x80 == 0 {
                blocks.push(RedBlock {
                    ts_offset: 0,
                    length: 0,
                    pt: payload[0] & 0x7F,
                    primary: true,
                });
                payload = &payload[1..];
                break;
            }

            if payload.len() < 4 {
                return Err(RedError::IncompleteHeader);
            }

            let block_head = u32::from_be_bytes([payload[0], payload[1], payload[2], payload[3]]);
            let length = (block_head & 0x03FF) as usize;
            let shifted = block_head >> 10;
            let ts_offset = shifted & 0x3FFF;
            let pt = ((shifted >> 14) & 0x7F) as u8;

            blocks.push(RedBlock {
                ts_offset,
                length,
                pt,
                primary: false,
            });
            block_length += length;
            payload = &payload[4..];
        }

        if payload.len() < block_length {
            return Err(RedError::IncompleteBlock);
        }

        let mut pkts = Vec::<RtpPacketLite>::new();
        for (i, b) in blocks.iter().enumerate() {
            if b.primary {
                pkts.push(RtpPacketLite::with_payload(
                    red_pkt.sequence_number,
                    red_pkt.timestamp,
                    b.pt,
                    payload.to_vec(),
                ));
                break;
            }

            let recover_index = blocks.len() - i - 1;
            if recover_index < 1 || (recover_bits & (1 << (recover_index - 1))) == 0 {
                payload = &payload[b.length..];
                continue;
            }

            pkts.push(RtpPacketLite::with_payload(
                red_pkt.sequence_number.wrapping_sub(recover_index as u16),
                red_pkt.timestamp.wrapping_sub(b.ts_offset),
                b.pt,
                payload[..b.length].to_vec(),
            ));
            payload = &payload[b.length..];
        }

        Ok(pkts)
    }

    fn extract_primary_encoding_for_red(mut payload: &[u8]) -> Result<Vec<u8>, RedError> {
        let mut block_length = 0usize;

        loop {
            if payload.is_empty() {
                return Err(RedError::IncompleteHeader);
            }

            if payload[0] & 0x80 == 0 {
                payload = &payload[1..];
                break;
            }

            if payload.len() < 4 {
                return Err(RedError::IncompleteHeader);
            }

            let len_field = u16::from_be_bytes([payload[2], payload[3]]) & 0x03FF;
            block_length += len_field as usize;
            payload = &payload[4..];
        }

        if payload.len() < block_length {
            return Err(RedError::IncompleteBlock);
        }

        Ok(payload[block_length..].to_vec())
    }

    fn generate_pkts(
        mut sequence_number: u16,
        mut timestamp: u32,
        count: usize,
        ts_step: u32,
    ) -> Vec<RtpPacketLite> {
        let mut pkts = Vec::with_capacity(count);
        for _ in 0..count {
            let mut payload = Vec::with_capacity(6);
            payload.extend_from_slice(&sequence_number.to_be_bytes());
            payload.extend_from_slice(&timestamp.to_be_bytes());
            pkts.push(RtpPacketLite::with_payload(
                sequence_number,
                timestamp,
                OPUS_PT,
                payload,
            ));
            sequence_number = sequence_number.wrapping_add(1);
            timestamp = timestamp.wrapping_add(ts_step);
        }
        pkts
    }

    fn generate_red_pkts(pkts: &[RtpPacketLite], red_count: usize) -> Vec<RtpPacketLite> {
        let mut red_pkts = Vec::with_capacity(pkts.len());
        for i in 0..pkts.len() {
            let start = i.saturating_sub(red_count);
            let prior = &pkts[start..i];
            let mut buf = vec![0u8; MTU_SIZE];
            let encoded = encode_red_for_primary_blocks(prior, &pkts[i], &mut buf)
                .expect("red encoding should succeed");
            red_pkts.push(RtpPacketLite::with_payload(
                pkts[i].sequence_number,
                pkts[i].timestamp,
                OPUS_RED_PT,
                buf[..encoded].to_vec(),
            ));
        }
        red_pkts
    }

    fn verify_encoding_equal(actual: &RtpPacketLite, expected: &RtpPacketLite) {
        assert_eq!(actual.timestamp, expected.timestamp);
        assert_eq!(actual.payload_type, expected.payload_type);
        assert_eq!(actual.payload, expected.payload);
    }

    fn verify_pkts_equal(actual: &[RtpPacketLite], expected: &[RtpPacketLite]) {
        assert_eq!(actual.len(), expected.len());
        for (actual, expected) in actual.iter().zip(expected.iter()) {
            verify_encoding_equal(actual, expected);
        }
    }

    // Upstream: livekit/pkg/sfu/redreceiver_test.go::TestRedReceiver
    #[test]
    fn red_receiver_matches_upstream_contract() {
        let mut receiver = RedReceiverLite::new();
        let header_seq = 65534u16;
        let header_ts = (1u32 << 31) - 2 * TS_STEP;

        // normal
        let mut expected = Vec::<RtpPacketLite>::new();
        for pkt in generate_pkts(header_seq, header_ts, 10, TS_STEP) {
            expected.push(pkt.clone());
            if expected.len() > MAX_RED_COUNT + 1 {
                expected.remove(0);
            }
            let mut red_payload = vec![0u8; MTU_SIZE];
            let encoded = receiver
                .encode_red_for_primary(&pkt, &mut red_payload)
                .expect("red encode should succeed");
            let red_pkt = RtpPacketLite::with_payload(
                pkt.sequence_number,
                pkt.timestamp,
                OPUS_RED_PT,
                red_payload[..encoded].to_vec(),
            );
            let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
            verify_pkts_equal(
                &decoded,
                &expected
                    .iter()
                    .cloned()
                    .filter(|p| !p.payload.is_empty())
                    .collect::<Vec<_>>(),
            );
        }

        // packet lost and jump
        let mut receiver = RedReceiverLite::new();
        let mut sequence_number = header_seq;
        let mut timestamp = header_ts;
        let mut expected = Vec::<Option<RtpPacketLite>>::new();
        for i in 0..10usize {
            if i % 2 == 0 {
                sequence_number = sequence_number.wrapping_add(1);
                timestamp = timestamp.wrapping_add(TS_STEP);
                expected.push(None);
                continue;
            }

            let pkt = RtpPacketLite::with_payload(
                sequence_number,
                timestamp,
                OPUS_PT,
                vec![0xAB, 0xCD, (sequence_number & 0xFF) as u8],
            );
            expected.push(Some(pkt.clone()));
            if expected.len() > MAX_RED_COUNT + 1 {
                expected = expected[expected.len() - (MAX_RED_COUNT + 1)..].to_vec();
            }

            let mut red_payload = vec![0u8; MTU_SIZE];
            let encoded = receiver
                .encode_red_for_primary(&pkt, &mut red_payload)
                .expect("red encode should succeed");
            let red_pkt = RtpPacketLite::with_payload(
                pkt.sequence_number,
                pkt.timestamp,
                OPUS_RED_PT,
                red_payload[..encoded].to_vec(),
            );
            let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
            let expected_solid = expected.iter().flatten().cloned().collect::<Vec<_>>();
            verify_pkts_equal(&decoded, &expected_solid);

            sequence_number = sequence_number.wrapping_add(1);
            timestamp = timestamp.wrapping_add(TS_STEP);
        }

        // jump clears recoverable history
        sequence_number = sequence_number.wrapping_add(10);
        timestamp = timestamp.wrapping_add(10 * TS_STEP);
        let mut expected_jump = Vec::<RtpPacketLite>::new();
        for pkt in generate_pkts(sequence_number, timestamp, 3, TS_STEP) {
            expected_jump.push(pkt.clone());
            if expected_jump.len() > MAX_RED_COUNT + 1 {
                expected_jump.remove(0);
            }
            let mut red_payload = vec![0u8; MTU_SIZE];
            let encoded = receiver
                .encode_red_for_primary(&pkt, &mut red_payload)
                .expect("red encode should succeed");
            let red_pkt = RtpPacketLite::with_payload(
                pkt.sequence_number,
                pkt.timestamp,
                OPUS_RED_PT,
                red_payload[..encoded].to_vec(),
            );
            let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
            verify_pkts_equal(&decoded, &expected_jump);
        }

        // out-of-order and repeat
        let mut receiver = RedReceiverLite::new();
        let prev_pkts = generate_pkts(header_seq, header_ts, 10, TS_STEP);
        for pkt in &prev_pkts {
            let mut red_payload = vec![0u8; MTU_SIZE];
            let _ = receiver
                .encode_red_for_primary(pkt, &mut red_payload)
                .expect("seed history should succeed");
        }

        let old_unordered = prev_pkts[7].clone();
        let mut red_payload = vec![0u8; MTU_SIZE];
        let encoded = receiver
            .encode_red_for_primary(&old_unordered, &mut red_payload)
            .expect("encode should succeed");
        let red_pkt = RtpPacketLite::with_payload(
            old_unordered.sequence_number,
            old_unordered.timestamp,
            OPUS_RED_PT,
            red_payload[..encoded].to_vec(),
        );
        let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
        verify_pkts_equal(&decoded, &[prev_pkts[7].clone()]);

        let repeated = prev_pkts[9].clone();
        let mut red_payload = vec![0u8; MTU_SIZE];
        let encoded = receiver
            .encode_red_for_primary(&repeated, &mut red_payload)
            .expect("encode should succeed");
        let red_pkt = RtpPacketLite::with_payload(
            repeated.sequence_number,
            repeated.timestamp,
            OPUS_RED_PT,
            red_payload[..encoded].to_vec(),
        );
        let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
        verify_pkts_equal(&decoded, &[prev_pkts[8].clone(), prev_pkts[9].clone()]);

        // encoding exceed space falls back to primary only
        let mut receiver = RedReceiverLite::new();
        for mut pkt in generate_pkts(header_seq, header_ts, 10, TS_STEP) {
            pkt.payload = vec![0u8; 1000];
            let mut red_payload = vec![0u8; MTU_SIZE];
            let encoded = receiver
                .encode_red_for_primary(&pkt, &mut red_payload)
                .expect("encode should succeed");
            let red_pkt = RtpPacketLite::with_payload(
                pkt.sequence_number,
                pkt.timestamp,
                OPUS_RED_PT,
                red_payload[..encoded].to_vec(),
            );
            let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
            verify_pkts_equal(&decoded, &[pkt]);
        }

        // large timestamp gap yields primary only
        let mut receiver = RedReceiverLite::new();
        let mut expected = Vec::<RtpPacketLite>::new();
        for pkt in generate_pkts(header_seq, header_ts, 4, TS_STEP) {
            expected.push(pkt.clone());
            if expected.len() > MAX_RED_COUNT + 1 {
                expected.remove(0);
            }
            let mut red_payload = vec![0u8; MTU_SIZE];
            let encoded = receiver
                .encode_red_for_primary(&pkt, &mut red_payload)
                .expect("encode should succeed");
            let red_pkt = RtpPacketLite::with_payload(
                pkt.sequence_number,
                pkt.timestamp,
                OPUS_RED_PT,
                red_payload[..encoded].to_vec(),
            );
            let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
            verify_pkts_equal(&decoded, &expected);
        }
        for pkt in generate_pkts(header_seq, header_ts, 4, 40 * TS_STEP) {
            let mut red_payload = vec![0u8; MTU_SIZE];
            let encoded = receiver
                .encode_red_for_primary(&pkt, &mut red_payload)
                .expect("encode should succeed");
            let red_pkt = RtpPacketLite::with_payload(
                pkt.sequence_number,
                pkt.timestamp,
                OPUS_RED_PT,
                red_payload[..encoded].to_vec(),
            );
            let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");
            verify_pkts_equal(&decoded, &[pkt]);
        }
    }

    fn test_red_primary_receiver(
        max_pkt_count: usize,
        red_count: usize,
        send_pkt_idx: &[usize],
        expect_pkt_idx: &[usize],
    ) {
        let primary_pkts = generate_pkts(65530, (1u32 << 31) - 2 * TS_STEP, max_pkt_count, TS_STEP);
        let red_pkts = generate_red_pkts(&primary_pkts, red_count);

        let mut receiver = RedPrimaryReceiverLite::new(OPUS_RED_PT);
        let mut received = Vec::<RtpPacketLite>::new();
        for idx in send_pkt_idx {
            let forwarded = receiver
                .forward_rtp(&red_pkts[*idx])
                .expect("red-primary forward should decode");
            received.extend(forwarded);
        }

        let expected = expect_pkt_idx
            .iter()
            .map(|idx| primary_pkts[*idx].clone())
            .collect::<Vec<_>>();
        verify_pkts_equal(&received, &expected);
    }

    // Upstream: livekit/pkg/sfu/redreceiver_test.go::TestRedPrimaryReceiver
    #[test]
    fn red_primary_receiver_matches_upstream_contract() {
        // packet should send only once
        let send = (0..19usize).collect::<Vec<_>>();
        test_red_primary_receiver(19, MAX_RED_COUNT, &send, &send);

        // packet duplicate and unorder
        let mut send = Vec::<usize>::new();
        for i in 0..19usize {
            send.push(i);
            if i > 0 {
                send.push(i - 1);
            }
            send.push(i);
        }
        test_red_primary_receiver(19, MAX_RED_COUNT, &send, &send);

        // full recover
        let mut send = Vec::<usize>::new();
        let mut recv = Vec::<usize>::new();
        for i in 0..19usize {
            recv.push(i);
            if i % (MAX_RED_COUNT + 1) == 0 {
                send.push(i);
            }
        }
        test_red_primary_receiver(19, MAX_RED_COUNT, &send, &recv);

        // lost 2 but red recover 1
        test_red_primary_receiver(19, 1, &[0, 3, 6, 9, 12], &[0, 2, 3, 5, 6, 8, 9, 11, 12]);

        // part recover and long jump
        test_red_primary_receiver(
            50,
            MAX_RED_COUNT,
            &[0, 5, 12, 21, 24, 27],
            &[0, 3, 4, 5, 10, 11, 12, 19, 20, 21, 22, 23, 24, 25, 26, 27],
        );

        // unorder
        test_red_primary_receiver(
            50,
            MAX_RED_COUNT,
            &[20, 10, 25, 23, 34],
            &[20, 10, 23, 24, 25, 21, 22, 23, 32, 33, 34],
        );

        // mixed primary codec should forward directly
        let mut receiver = RedPrimaryReceiverLite::new(OPUS_RED_PT);
        let primary_pkt = RtpPacketLite::with_payload(
            65530,
            (1u32 << 31) - 2 * TS_STEP,
            OPUS_PT,
            vec![1, 3, 5, 7, 9],
        );
        let forwarded = receiver
            .forward_rtp(&primary_pkt)
            .expect("non-red packet should forward");
        verify_pkts_equal(&forwarded, &[primary_pkt]);
    }

    // Upstream: livekit/pkg/sfu/redreceiver_test.go::TestExtractPrimaryEncodingForRED
    #[test]
    fn extract_primary_encoding_for_red_matches_upstream_contract() {
        let pkts = generate_pkts(65530, (1u32 << 31) - 2 * TS_STEP, 10, TS_STEP);
        let red_pkts = generate_red_pkts(&pkts, MAX_RED_COUNT);

        let mut primary_pkts = Vec::<RtpPacketLite>::with_capacity(red_pkts.len());
        for red_pkt in &red_pkts {
            let payload = extract_primary_encoding_for_red(&red_pkt.payload)
                .expect("primary extraction should succeed");
            primary_pkts.push(RtpPacketLite::with_payload(
                red_pkt.sequence_number,
                red_pkt.timestamp,
                OPUS_PT,
                payload,
            ));
        }

        verify_pkts_equal(&primary_pkts, &pkts);
    }

    #[test]
    fn red_receiver_falls_back_to_primary_when_payload_exceeds_redundant_length_contract() {
        let mut receiver = RedReceiverLite::new();
        let pkt = RtpPacketLite::with_payload(400, 9000, OPUS_PT, vec![0u8; MAX_RED_PAYLOAD]);

        let mut red_payload = vec![0u8; MTU_SIZE];
        let encoded = receiver
            .encode_red_for_primary(&pkt, &mut red_payload)
            .expect("encode should succeed");
        let red_pkt = RtpPacketLite::with_payload(
            pkt.sequence_number,
            pkt.timestamp,
            OPUS_RED_PT,
            red_payload[..encoded].to_vec(),
        );
        let decoded = extract_pkts_from_red(&red_pkt, 0xFF).expect("decode should succeed");

        verify_pkts_equal(&decoded, &[pkt]);
    }
}
