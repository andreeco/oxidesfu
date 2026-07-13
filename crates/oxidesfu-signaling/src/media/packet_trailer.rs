pub(crate) const PACKET_TRAILER_MAGIC: [u8; 4] = [b'L', b'K', b'T', b'S'];

const XOR_BYTE: u8 = 0xFF;
const ENVELOPE_SIZE: usize = 5; // 1B trailer_len + 4B magic

/// Returns the number of bytes to strip from the end of an RTP payload
/// when a valid LKTS trailer suffix is present.
#[allow(dead_code)]
pub(crate) fn strip_packet_trailer(payload: &[u8], marker: bool) -> usize {
    if !marker || payload.len() < ENVELOPE_SIZE {
        return 0;
    }

    let tail = &payload[payload.len() - 4..];
    if tail != PACKET_TRAILER_MAGIC {
        return 0;
    }

    let trailer_len = (payload[payload.len() - 5] ^ XOR_BYTE) as usize;
    if trailer_len < ENVELOPE_SIZE || trailer_len > payload.len() {
        return 0;
    }

    trailer_len
}

#[cfg(test)]
mod tests {
    use super::{ENVELOPE_SIZE, PACKET_TRAILER_MAGIC, XOR_BYTE, strip_packet_trailer};

    const TAG_TIMESTAMP_US: u8 = 0x01;
    const TAG_FRAME_ID: u8 = 0x02;

    fn append_tlv(mut dst: Vec<u8>, tag: u8, value: &[u8]) -> Vec<u8> {
        dst.push(tag ^ XOR_BYTE);
        dst.push((value.len() as u8) ^ XOR_BYTE);
        dst.extend(value.iter().map(|byte| byte ^ XOR_BYTE));
        dst
    }

    fn append_envelope(mut dst: Vec<u8>, trailer_len: u8) -> Vec<u8> {
        dst.push(trailer_len ^ XOR_BYTE);
        dst.extend_from_slice(&PACKET_TRAILER_MAGIC);
        dst
    }

    fn make_trailer(timestamp_us: i64, frame_id: u32) -> Vec<u8> {
        let mut trailer = Vec::new();

        trailer = append_tlv(
            trailer,
            TAG_TIMESTAMP_US,
            &(timestamp_us as u64).to_be_bytes(),
        );
        trailer = append_tlv(trailer, TAG_FRAME_ID, &frame_id.to_be_bytes());

        let trailer_len = (trailer.len() + ENVELOPE_SIZE) as u8;
        append_envelope(trailer, trailer_len)
    }

    fn make_payload_with_trailer(video_len: usize, timestamp_us: i64, frame_id: u32) -> Vec<u8> {
        let mut video = vec![0_u8; video_len];
        for (index, byte) in video.iter_mut().enumerate() {
            *byte = index as u8;
        }
        video.extend(make_trailer(timestamp_us, frame_id));
        video
    }

    fn make_timestamp_only_trailer(timestamp_us: i64) -> Vec<u8> {
        let mut trailer = Vec::new();
        trailer = append_tlv(
            trailer,
            TAG_TIMESTAMP_US,
            &(timestamp_us as u64).to_be_bytes(),
        );
        let trailer_len = (trailer.len() + ENVELOPE_SIZE) as u8;
        append_envelope(trailer, trailer_len)
    }

    #[test]
    fn strip_packet_trailer_matches_upstream_contract() {
        let full_trailer_size = 21usize; // (1+1+8) + (1+1+4) + 5
        let timestamp_only_trailer_size = 15usize; // (1+1+8) + 5

        let test_cases: Vec<(&str, Vec<u8>, bool, usize)> = vec![
            (
                "marker set with full trailer",
                make_payload_with_trailer(20, 1_700_000_000_000_000, 42),
                true,
                full_trailer_size,
            ),
            (
                "marker set with timestamp-only trailer",
                {
                    let mut video = vec![0_u8; 20];
                    video.extend(make_timestamp_only_trailer(1_700_000_000_000_000));
                    video
                },
                true,
                timestamp_only_trailer_size,
            ),
            (
                "marker not set with valid trailer",
                make_payload_with_trailer(20, 1_700_000_000_000_000, 42),
                false,
                0,
            ),
            ("marker set without magic", vec![0_u8; 32], true, 0),
            (
                "marker set but payload too short for envelope",
                vec![b'L', b'K', b'T', b'S'],
                true,
                0,
            ),
            (
                "marker set with partial magic mismatch",
                {
                    let mut payload = make_payload_with_trailer(20, 1_700_000_000_000_000, 42);
                    let len = payload.len();
                    payload[len - 1] = b'x';
                    payload
                },
                true,
                0,
            ),
            (
                "trailer_len exceeds payload length",
                {
                    let mut trailer = Vec::new();
                    trailer = append_tlv(trailer, TAG_TIMESTAMP_US, &42_u64.to_be_bytes());
                    append_envelope(trailer, 200)
                },
                true,
                0,
            ),
            (
                "trailer_len smaller than envelope",
                {
                    let mut video = vec![0_u8; 20];
                    let mut trailer = Vec::new();
                    trailer = append_tlv(trailer, TAG_TIMESTAMP_US, &42_u64.to_be_bytes());
                    trailer = append_envelope(trailer, 3);
                    video.extend(trailer);
                    video
                },
                true,
                0,
            ),
            (
                "exactly envelope-only trailer",
                append_envelope(Vec::new(), ENVELOPE_SIZE as u8),
                true,
                ENVELOPE_SIZE,
            ),
            ("empty payload", Vec::new(), true, 0),
        ];

        for (name, payload, marker, expected) in test_cases {
            let actual = strip_packet_trailer(&payload, marker);
            assert_eq!(actual, expected, "case `{name}`");
        }
    }
}
