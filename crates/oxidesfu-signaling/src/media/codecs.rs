use livekit_protocol as proto;

fn mime_type_equal(a: &str, b: &str) -> bool {
    a.trim().eq_ignore_ascii_case(b.trim())
}

pub(crate) fn is_codec_enabled(
    enabled_codecs: &[proto::Codec],
    mime_type: &str,
    sdp_fmtp_line: &str,
) -> bool {
    enabled_codecs.iter().any(|codec| {
        mime_type_equal(&codec.mime, mime_type)
            && (codec.fmtp_line.trim().is_empty()
                || codec.fmtp_line.eq_ignore_ascii_case(sdp_fmtp_line))
    })
}

#[cfg(test)]
mod tests {
    use livekit_protocol as proto;

    use super::is_codec_enabled;

    #[test]
    fn is_codec_enabled_empty_fmtp_requirement_matches_any_matching_mime() {
        let enabled_codecs = vec![proto::Codec {
            mime: "video/h264".to_string(),
            fmtp_line: String::new(),
        }];

        assert!(is_codec_enabled(&enabled_codecs, "video/H264", "special"));
        assert!(is_codec_enabled(&enabled_codecs, "video/h264", ""));
        assert!(!is_codec_enabled(&enabled_codecs, "video/vp8", "special"));
    }

    #[test]
    fn is_codec_enabled_fmtp_requirement_must_match_when_provided() {
        let enabled_codecs = vec![proto::Codec {
            mime: "video/h264".to_string(),
            fmtp_line: "special".to_string(),
        }];

        assert!(is_codec_enabled(&enabled_codecs, "video/h264", "special"));
        assert!(!is_codec_enabled(&enabled_codecs, "video/h264", ""));
        assert!(!is_codec_enabled(&enabled_codecs, "video/vp8", "special"));
    }
}
