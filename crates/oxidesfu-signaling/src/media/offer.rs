use std::collections::{HashMap, HashSet};

fn track_id_from_msid_value(msid_value: &str) -> Option<String> {
    let mut parts = msid_value.split_whitespace();
    let _stream_id = parts.next();
    let track_id = parts.next()?;
    Some(
        track_id
            .split_once('|')
            .map(|(_, track_sid)| track_sid)
            .unwrap_or(track_id)
            .to_string(),
    )
}

fn track_id_from_sdp_line(line: &str) -> Option<String> {
    if let Some(msid) = line.strip_prefix("a=msid:") {
        return track_id_from_msid_value(msid);
    }

    if line.starts_with("a=ssrc:")
        && let Some((_, msid)) = line.split_once(" msid:")
    {
        return track_id_from_msid_value(msid);
    }

    None
}

fn section_line_has_msid(line: &str) -> bool {
    line.starts_with("a=msid:") || (line.starts_with("a=ssrc:") && line.contains(" msid:"))
}

pub(crate) fn mid_to_track_id_from_offer_sdp(offer_sdp: &str) -> HashMap<String, String> {
    let mut current_mid: Option<String> = None;
    let mut current_direction = "sendrecv".to_string();
    let mut current_track_id: Option<String> = None;
    let mut mapping = HashMap::new();

    let mut flush_section = |mid: &Option<String>, direction: &str, track_id: &Option<String>| {
        let sends_media = matches!(direction, "sendrecv" | "sendonly");
        if !sends_media {
            return;
        }
        let (Some(mid), Some(track_id)) = (mid, track_id) else {
            return;
        };
        mapping.insert(mid.clone(), track_id.clone());
    };

    for line in offer_sdp.lines() {
        if line.starts_with("m=") {
            flush_section(&current_mid, &current_direction, &current_track_id);
            current_mid = None;
            current_direction = "sendrecv".to_string();
            current_track_id = None;
            continue;
        }

        if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(mid.trim().to_string());
            continue;
        }

        if line == "a=sendrecv"
            || line == "a=sendonly"
            || line == "a=recvonly"
            || line == "a=inactive"
        {
            current_direction = line.trim_start_matches("a=").to_string();
            continue;
        }

        if let Some(track_id) = track_id_from_sdp_line(line) {
            current_track_id = Some(track_id);
        }
    }

    flush_section(&current_mid, &current_direction, &current_track_id);
    mapping
}

pub(crate) fn mid_to_track_id_from_answer_sdp(sdp: &str) -> HashMap<String, String> {
    let mut current_mid: Option<String> = None;
    let mut mapping = HashMap::new();

    for line in sdp.lines() {
        if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(mid.trim().to_string());
            continue;
        }

        let Some(mid) = current_mid.clone() else {
            continue;
        };

        if let Some(track_id) = track_id_from_sdp_line(line) {
            mapping.insert(mid, track_id);
        }
    }

    mapping
}

pub(crate) fn accepted_media_mids_from_answer_sdp(answer_sdp: &str) -> HashSet<String> {
    let mut accepted_mids = HashSet::new();
    let mut current_mid: Option<String> = None;
    let mut current_direction = "sendrecv".to_string();
    let mut current_is_rejected = false;

    let mut flush_section = |mid: &Option<String>, direction: &str, is_rejected: bool| {
        let Some(mid) = mid else {
            return;
        };
        if is_rejected || direction == "inactive" {
            return;
        }
        accepted_mids.insert(mid.clone());
    };

    for line in answer_sdp.lines() {
        if let Some(media_line) = line.strip_prefix("m=") {
            flush_section(&current_mid, &current_direction, current_is_rejected);
            let mut parts = media_line.split_whitespace();
            let _media = parts.next();
            current_is_rejected = parts.next().and_then(|port| port.parse::<u16>().ok()) == Some(0);
            current_mid = None;
            current_direction = "sendrecv".to_string();
            continue;
        }

        if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(mid.trim().to_string());
            continue;
        }

        if line == "a=sendrecv"
            || line == "a=sendonly"
            || line == "a=recvonly"
            || line == "a=inactive"
        {
            current_direction = line.trim_start_matches("a=").to_string();
        }
    }

    flush_section(&current_mid, &current_direction, current_is_rejected);
    accepted_mids
}

#[cfg(test)]
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub(crate) struct ReceiveSectionCounts {
    pub(crate) audio: usize,
    pub(crate) video: usize,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ReceiveSection {
    pub(crate) mid: String,
    pub(crate) kind: ReceiveSectionKind,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum ReceiveSectionKind {
    Audio,
    Video,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct OfferMediaSection {
    pub(crate) mid: String,
    pub(crate) kind: Option<ReceiveSectionKind>,
    pub(crate) direction: String,
    pub(crate) has_msid: bool,
    pub(crate) is_rejected: bool,
}

pub(crate) fn offer_media_sections_from_sdp(offer_sdp: &str) -> Vec<OfferMediaSection> {
    let mut sections = Vec::new();
    let mut current_media: Option<&str> = None;
    let mut current_mid: Option<String> = None;
    let mut current_direction = "sendrecv".to_string();
    let mut current_has_msid = false;
    let mut current_is_rejected = false;

    let mut flush_section = |media: Option<&str>,
                             mid: &Option<String>,
                             direction: &str,
                             has_msid: bool,
                             is_rejected: bool| {
        let Some(mid) = mid.clone() else {
            return;
        };
        let kind = match media {
            Some("audio") => Some(ReceiveSectionKind::Audio),
            Some("video") => Some(ReceiveSectionKind::Video),
            _ => None,
        };
        sections.push(OfferMediaSection {
            mid,
            kind,
            direction: direction.to_string(),
            has_msid,
            is_rejected,
        });
    };

    for line in offer_sdp.lines() {
        if let Some(media_line) = line.strip_prefix("m=") {
            flush_section(
                current_media,
                &current_mid,
                &current_direction,
                current_has_msid,
                current_is_rejected,
            );
            let mut parts = media_line.split_whitespace();
            current_media = parts.next();
            current_is_rejected = parts.next().and_then(|port| port.parse::<u16>().ok()) == Some(0);
            current_mid = None;
            current_direction = "sendrecv".to_string();
            current_has_msid = false;
            continue;
        }

        if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(mid.trim().to_string());
            continue;
        }

        if line == "a=sendrecv"
            || line == "a=sendonly"
            || line == "a=recvonly"
            || line == "a=inactive"
        {
            current_direction = line.trim_start_matches("a=").to_string();
            continue;
        }

        if section_line_has_msid(line) {
            current_has_msid = true;
        }
    }

    flush_section(
        current_media,
        &current_mid,
        &current_direction,
        current_has_msid,
        current_is_rejected,
    );

    sections
}

#[allow(dead_code)]
pub(crate) fn receive_sections_from_offer(offer_sdp: &str) -> Vec<ReceiveSection> {
    offer_media_sections_from_sdp(offer_sdp)
        .into_iter()
        .filter_map(|section| {
            if section.is_rejected {
                return None;
            }
            let can_receive_downtrack = matches!(section.direction.as_str(), "recvonly")
                || (matches!(section.direction.as_str(), "sendrecv") && !section.has_msid);
            if !can_receive_downtrack {
                return None;
            }
            section.kind.map(|kind| ReceiveSection {
                mid: section.mid,
                kind,
            })
        })
        .collect()
}

#[cfg(test)]
pub(crate) fn receive_section_counts_from_offer(offer_sdp: &str) -> ReceiveSectionCounts {
    receive_sections_from_offer(offer_sdp).into_iter().fold(
        ReceiveSectionCounts::default(),
        |mut counts, section| {
            match section.kind {
                ReceiveSectionKind::Audio => counts.audio += 1,
                ReceiveSectionKind::Video => counts.video += 1,
            }
            counts
        },
    )
}

pub(crate) fn receive_supported_video_mime_types_from_offer(offer_sdp: &str) -> HashSet<String> {
    #[derive(Default)]
    struct MediaSectionState {
        media: Option<String>,
        direction: String,
        has_msid: bool,
        is_rejected: bool,
        payload_types: HashSet<String>,
        payload_codecs: HashMap<String, String>,
    }

    impl MediaSectionState {
        fn new() -> Self {
            Self {
                direction: "sendrecv".to_string(),
                ..Default::default()
            }
        }

        fn reset_for_media_line(&mut self, media_line: &str) {
            let mut parts = media_line.split_whitespace();
            self.media = parts.next().map(|value| value.to_string());
            self.is_rejected = parts.next().and_then(|port| port.parse::<u16>().ok()) == Some(0);
            self.direction = "sendrecv".to_string();
            self.has_msid = false;
            self.payload_types = parts.map(ToOwned::to_owned).collect();
            self.payload_codecs.clear();
        }

        fn supports_receive_video(&self) -> bool {
            if self.is_rejected {
                return false;
            }
            if self.media.as_deref() != Some("video") {
                return false;
            }
            matches!(self.direction.as_str(), "recvonly")
                || (matches!(self.direction.as_str(), "sendrecv") && !self.has_msid)
        }
    }

    fn flush_section(section: &MediaSectionState, codecs: &mut HashSet<String>) {
        if !section.supports_receive_video() {
            return;
        }

        for payload_type in &section.payload_types {
            let Some(codec) = section.payload_codecs.get(payload_type) else {
                continue;
            };
            codecs.insert(format!("video/{}", codec.to_ascii_lowercase()));
        }
    }

    let mut codecs = HashSet::new();
    let mut section = MediaSectionState::new();

    for line in offer_sdp.lines() {
        if let Some(media_line) = line.strip_prefix("m=") {
            flush_section(&section, &mut codecs);
            section.reset_for_media_line(media_line);
            continue;
        }

        if line == "a=sendrecv"
            || line == "a=sendonly"
            || line == "a=recvonly"
            || line == "a=inactive"
        {
            section.direction = line.trim_start_matches("a=").to_string();
            continue;
        }

        if section_line_has_msid(line) {
            section.has_msid = true;
            continue;
        }

        if let Some(rtpmap) = line.strip_prefix("a=rtpmap:") {
            let Some((payload_type, codec_rate)) = rtpmap.split_once(' ') else {
                continue;
            };
            let codec = codec_rate
                .split('/')
                .next()
                .map(str::trim)
                .filter(|value| !value.is_empty())
                .map(ToOwned::to_owned);
            if let Some(codec) = codec {
                section
                    .payload_codecs
                    .insert(payload_type.trim().to_string(), codec);
            }
        }
    }

    flush_section(&section, &mut codecs);
    codecs
}

pub(crate) fn active_publisher_mids_from_offer(offer_sdp: &str) -> HashSet<String> {
    let mut active_mids = HashSet::new();

    let mut current_mid: Option<String> = None;
    let mut current_direction = "sendrecv".to_string();
    let mut current_has_msid = false;
    let mut current_is_rejected = false;

    let mut flush_section =
        |mid: &Option<String>, direction: &str, has_msid: bool, is_rejected: bool| {
            if is_rejected {
                return;
            }
            let is_publish_section =
                matches!(direction, "sendonly") || (matches!(direction, "sendrecv") && has_msid);
            if is_publish_section && let Some(mid) = mid {
                active_mids.insert(mid.clone());
            }
        };

    for line in offer_sdp.lines() {
        if let Some(media_line) = line.strip_prefix("m=") {
            flush_section(
                &current_mid,
                &current_direction,
                current_has_msid,
                current_is_rejected,
            );
            let mut parts = media_line.split_whitespace();
            let _media = parts.next();
            current_is_rejected = parts.next().and_then(|port| port.parse::<u16>().ok()) == Some(0);
            current_mid = None;
            current_direction = "sendrecv".to_string();
            current_has_msid = false;
            continue;
        }

        if let Some(mid) = line.strip_prefix("a=mid:") {
            current_mid = Some(mid.trim().to_string());
            continue;
        }

        if line == "a=sendrecv"
            || line == "a=sendonly"
            || line == "a=recvonly"
            || line == "a=inactive"
        {
            current_direction = line.trim_start_matches("a=").to_string();
            continue;
        }

        if section_line_has_msid(line) {
            current_has_msid = true;
        }
    }

    flush_section(
        &current_mid,
        &current_direction,
        current_has_msid,
        current_is_rejected,
    );
    active_mids
}

#[cfg(test)]
mod tests {
    use super::{
        active_publisher_mids_from_offer, mid_to_track_id_from_offer_sdp,
        receive_supported_video_mime_types_from_offer,
    };

    #[test]
    fn parses_mid_to_track_id_from_ssrc_msid_lines() {
        let offer = "v=0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:1\r\n\
a=sendrecv\r\n\
a=ssrc:1234 msid:streamA trackA\r\n";

        let mapping = mid_to_track_id_from_offer_sdp(offer);
        assert_eq!(mapping.get("1").map(String::as_str), Some("trackA"));
    }

    #[test]
    fn marks_publish_mid_active_from_ssrc_msid_lines() {
        let offer = "v=0\r\n\
m=audio 9 UDP/TLS/RTP/SAVPF 111\r\n\
a=mid:1\r\n\
a=sendrecv\r\n\
a=ssrc:1234 msid:streamA trackA\r\n";

        let mids = active_publisher_mids_from_offer(offer);
        assert!(mids.contains("1"));
    }

    #[test]
    fn parses_receive_supported_video_codecs_from_recvonly_section() {
        let offer = "v=0\r\n\
m=video 9 UDP/TLS/RTP/SAVPF 96 102\r\n\
a=mid:2\r\n\
a=recvonly\r\n\
a=rtpmap:96 VP8/90000\r\n\
a=rtpmap:102 H264/90000\r\n";

        let codecs = receive_supported_video_mime_types_from_offer(offer);
        assert!(codecs.contains("video/vp8"));
        assert!(codecs.contains("video/h264"));
    }

    #[test]
    fn ignores_publish_video_section_when_deriving_receive_supported_codecs() {
        let offer = "v=0\r\n\
m=video 9 UDP/TLS/RTP/SAVPF 96 102\r\n\
a=mid:3\r\n\
a=sendrecv\r\n\
a=msid:stream track\r\n\
a=rtpmap:96 VP8/90000\r\n\
a=rtpmap:102 H264/90000\r\n";

        let codecs = receive_supported_video_mime_types_from_offer(offer);
        assert!(codecs.is_empty());
    }
}
