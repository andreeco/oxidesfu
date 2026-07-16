#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub(crate) enum VideoQualityLite {
    Off,
    Low,
    Medium,
    High,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct VideoLayerLite {
    pub(crate) quality: VideoQualityLite,
    pub(crate) spatial_layer: i32,
    pub(crate) rid: String,
}

#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub(crate) struct TrackInfoLite {
    pub(crate) layers: Vec<VideoLayerLite>,
}

pub(crate) type VideoLayersRid = [String; 3];

pub(crate) const INVALID_LAYER_SPATIAL: i32 = -1;
const LAYER_SELECTION_TOLERANCE: f32 = 0.9;

fn qhf_rids() -> VideoLayersRid {
    ["q".to_string(), "h".to_string(), "f".to_string()]
}

fn two_one_zero_rids() -> VideoLayersRid {
    ["2".to_string(), "1".to_string(), "0".to_string()]
}

fn has_quality(layer_presence: &[bool; 3], quality: VideoQualityLite) -> bool {
    match quality {
        VideoQualityLite::Low => layer_presence[0],
        VideoQualityLite::Medium => layer_presence[1],
        VideoQualityLite::High => layer_presence[2],
        VideoQualityLite::Off => false,
    }
}

fn layer_presence(track_info: Option<&TrackInfoLite>) -> Option<[bool; 3]> {
    let track_info = track_info?;
    if track_info.layers.is_empty() {
        return None;
    }

    let mut presence = [false; 3];
    for layer in &track_info.layers {
        match layer.quality {
            VideoQualityLite::Low => presence[0] = true,
            VideoQualityLite::Medium => presence[1] = true,
            VideoQualityLite::High => presence[2] = true,
            VideoQualityLite::Off => {}
        }
    }
    Some(presence)
}

#[allow(dead_code)]
pub(crate) fn default_video_layers_rid() -> VideoLayersRid {
    qhf_rids()
}

#[allow(dead_code)]
pub(crate) fn rid_to_spatial_layer(
    rid: &str,
    track_info: Option<&TrackInfoLite>,
    rid_space: &VideoLayersRid,
) -> i32 {
    let Some(lp) = layer_presence(track_info) else {
        return match rid {
            "q" => 0,
            "h" => 1,
            "f" => 2,
            _ => 0,
        };
    };

    match rid {
        r if r == rid_space[0] => 0,
        r if r == rid_space[1] => {
            if (has_quality(&lp, VideoQualityLite::Medium)
                || has_quality(&lp, VideoQualityLite::High))
                && (has_quality(&lp, VideoQualityLite::Low)
                    || has_quality(&lp, VideoQualityLite::Medium)
                    || has_quality(&lp, VideoQualityLite::High))
            {
                1
            } else {
                0
            }
        }
        r if r == rid_space[2] => {
            if lp == [true, true, true] {
                2
            } else if (has_quality(&lp, VideoQualityLite::Low)
                && has_quality(&lp, VideoQualityLite::Medium))
                || (has_quality(&lp, VideoQualityLite::Low)
                    && has_quality(&lp, VideoQualityLite::High))
                || (has_quality(&lp, VideoQualityLite::Medium)
                    && has_quality(&lp, VideoQualityLite::High))
            {
                1
            } else {
                0
            }
        }
        _ => 0,
    }
}

#[allow(dead_code)]
pub(crate) fn spatial_layer_to_rid(
    layer: i32,
    track_info: Option<&TrackInfoLite>,
    rid_space: &VideoLayersRid,
) -> String {
    let Some(lp) = layer_presence(track_info) else {
        return match layer {
            0 => "q".to_string(),
            1 => "h".to_string(),
            2 => "f".to_string(),
            _ => "q".to_string(),
        };
    };

    match layer {
        0 => rid_space[0].clone(),
        1 => {
            if lp == [true, true, true]
                || lp == [true, true, false]
                || lp == [true, false, true]
                || lp == [false, true, true]
            {
                rid_space[1].clone()
            } else {
                rid_space[0].clone()
            }
        }
        2 => {
            if lp == [true, true, true] {
                rid_space[2].clone()
            } else if lp == [true, true, false]
                || lp == [true, false, true]
                || lp == [false, true, true]
            {
                rid_space[1].clone()
            } else {
                rid_space[0].clone()
            }
        }
        _ => rid_space[0].clone(),
    }
}

#[allow(dead_code)]
pub(crate) fn video_quality_to_spatial_layer(
    quality: VideoQualityLite,
    track_info: Option<&TrackInfoLite>,
) -> i32 {
    let Some(lp) = layer_presence(track_info) else {
        return match quality {
            VideoQualityLite::Low => 0,
            VideoQualityLite::Medium => 1,
            VideoQualityLite::High => 2,
            VideoQualityLite::Off => INVALID_LAYER_SPATIAL,
        };
    };

    match quality {
        VideoQualityLite::Low => 0,
        VideoQualityLite::Medium => {
            if lp == [false, true, true] {
                0
            } else if lp == [true, true, true]
                || lp == [true, true, false]
                || lp == [true, false, true]
            {
                1
            } else {
                0
            }
        }
        VideoQualityLite::High => {
            if lp == [true, true, true] {
                2
            } else if lp == [true, true, false]
                || lp == [true, false, true]
                || lp == [false, true, true]
            {
                1
            } else {
                0
            }
        }
        VideoQualityLite::Off => INVALID_LAYER_SPATIAL,
    }
}

#[allow(dead_code)]
pub(crate) fn spatial_layer_to_video_quality(
    layer: i32,
    track_info: Option<&TrackInfoLite>,
) -> VideoQualityLite {
    let Some(lp) = layer_presence(track_info) else {
        return match layer {
            0 => VideoQualityLite::Low,
            1 => VideoQualityLite::Medium,
            2 => VideoQualityLite::High,
            _ => VideoQualityLite::Off,
        };
    };

    match layer {
        0 => {
            if has_quality(&lp, VideoQualityLite::Low) {
                VideoQualityLite::Low
            } else if has_quality(&lp, VideoQualityLite::Medium) {
                VideoQualityLite::Medium
            } else {
                VideoQualityLite::High
            }
        }
        1 => {
            if lp == [true, true, true] || lp == [true, true, false] {
                VideoQualityLite::Medium
            } else {
                VideoQualityLite::High
            }
        }
        2 => VideoQualityLite::High,
        _ => VideoQualityLite::Off,
    }
}

#[allow(dead_code)]
pub(crate) fn video_quality_to_rid(
    quality: VideoQualityLite,
    track_info: Option<&TrackInfoLite>,
    rid_space: &VideoLayersRid,
) -> String {
    spatial_layer_to_rid(
        video_quality_to_spatial_layer(quality, track_info),
        track_info,
        rid_space,
    )
}

#[allow(dead_code)]
pub(crate) fn get_spatial_layer_for_rid(rid: &str, track_info: Option<&TrackInfoLite>) -> i32 {
    let Some(track_info) = track_info else {
        return INVALID_LAYER_SPATIAL;
    };

    if rid.is_empty() {
        return 0;
    }

    for layer in &track_info.layers {
        if layer.rid == rid {
            return layer.spatial_layer;
        }
    }

    if !track_info.layers.is_empty() {
        let has_any_rid = track_info.layers.iter().any(|layer| !layer.rid.is_empty());
        if !has_any_rid {
            return 0;
        }
    }

    0
}

#[allow(dead_code)]
pub(crate) fn get_spatial_layer_for_video_quality(
    quality: VideoQualityLite,
    track_info: Option<&TrackInfoLite>,
) -> i32 {
    let Some(track_info) = track_info else {
        return INVALID_LAYER_SPATIAL;
    };
    if quality == VideoQualityLite::Off {
        return INVALID_LAYER_SPATIAL;
    }

    for layer in &track_info.layers {
        if layer.quality == quality {
            return layer.spatial_layer;
        }
    }

    if track_info.layers.is_empty() {
        return 0;
    }

    video_quality_to_spatial_layer(quality, Some(track_info))
}

#[allow(dead_code)]
pub(crate) fn get_video_quality_for_spatial_layer(
    spatial_layer: i32,
    track_info: Option<&TrackInfoLite>,
) -> VideoQualityLite {
    if spatial_layer == INVALID_LAYER_SPATIAL || track_info.is_none() {
        return VideoQualityLite::Off;
    }

    let track_info = track_info.expect("checked above");
    for layer in &track_info.layers {
        if layer.spatial_layer == spatial_layer {
            return layer.quality;
        }
    }

    VideoQualityLite::Off
}

fn is_known_rids(rids: &VideoLayersRid, known: &VideoLayersRid) -> bool {
    rids.iter()
        .filter(|rid| !rid.is_empty())
        .all(|rid| known.contains(rid))
}

#[allow(dead_code)]
pub(crate) fn quality_for_dimension(
    track_width: u32,
    track_height: u32,
    requested_width: u32,
    requested_height: u32,
    mut layer_heights: Vec<u32>,
) -> VideoQualityLite {
    let mut quality = VideoQualityLite::High;

    if track_height == 0 && layer_heights.is_empty() {
        return quality;
    }

    let mut orig_size = track_height;
    let mut requested_size = requested_height;
    if track_width < track_height {
        orig_size = track_width;
        requested_size = requested_width;
    }

    if orig_size == 0
        && let Some(last_non_zero) = layer_heights.iter().rev().find(|height| **height > 0)
    {
        orig_size = *last_non_zero;
    }

    let mut layer_sizes = vec![180, 360, orig_size];

    if !layer_heights.is_empty() && layer_heights[0] > 0 {
        layer_sizes = std::mem::take(&mut layer_heights);
        // when explicit layer sizes are available, follow upstream behavior and compare heights.
        requested_size = requested_height;
        layer_sizes.sort_unstable();
    }

    requested_size = (requested_size as f32 * LAYER_SELECTION_TOLERANCE) as u32;
    for (index, size) in layer_sizes.iter().enumerate() {
        quality = match index {
            0 => VideoQualityLite::Low,
            1 => VideoQualityLite::Medium,
            _ => VideoQualityLite::High,
        };

        if index == layer_sizes.len().saturating_sub(1) {
            break;
        }

        if *size >= requested_size && *size != layer_sizes[index + 1] {
            break;
        }
    }

    quality
}

#[allow(dead_code)]
pub(crate) fn normalize_video_layers_rid(rids: &VideoLayersRid) -> VideoLayersRid {
    let mut out = rids.clone();

    let normalize = |known: &VideoLayersRid, out: &mut VideoLayersRid| {
        let mut index = 0usize;
        for expected in known {
            if rids.contains(expected) {
                out[index] = expected.clone();
                index += 1;
            }
        }
        while index < 3 {
            out[index].clear();
            index += 1;
        }
    };

    let qhf = qhf_rids();
    let two_one_zero = two_one_zero_rids();

    if is_known_rids(rids, &qhf) {
        normalize(&qhf, &mut out);
    }

    if is_known_rids(rids, &two_one_zero) {
        normalize(&two_one_zero, &mut out);
    }

    out
}

#[cfg(test)]
#[allow(clippy::type_complexity)]
mod tests {
    use std::collections::HashMap;

    use super::{
        INVALID_LAYER_SPATIAL, TrackInfoLite, VideoLayerLite, VideoLayersRid, VideoQualityLite,
        default_video_layers_rid, get_spatial_layer_for_rid, get_spatial_layer_for_video_quality,
        get_video_quality_for_spatial_layer, normalize_video_layers_rid, quality_for_dimension,
        rid_to_spatial_layer, spatial_layer_to_rid, spatial_layer_to_video_quality,
        video_quality_to_rid, video_quality_to_spatial_layer,
    };

    fn track(layers: &[(VideoQualityLite, i32, &str)]) -> TrackInfoLite {
        TrackInfoLite {
            layers: layers
                .iter()
                .map(|(quality, spatial_layer, rid)| VideoLayerLite {
                    quality: *quality,
                    spatial_layer: *spatial_layer,
                    rid: (*rid).to_string(),
                })
                .collect(),
        }
    }

    #[test]
    fn video_layer_utils_rid_conversion_matches_upstream_contract() {
        let tests: Vec<(Option<TrackInfoLite>, HashMap<&str, (&str, i32)>)> = vec![
            (
                None,
                HashMap::from([
                    ("", ("q", 0)),
                    ("q", ("q", 0)),
                    ("h", ("h", 1)),
                    ("f", ("f", 2)),
                ]),
            ),
            (
                Some(track(&[])),
                HashMap::from([
                    ("", ("q", 0)),
                    ("q", ("q", 0)),
                    ("h", ("h", 1)),
                    ("f", ("f", 2)),
                ]),
            ),
            (
                Some(track(&[(VideoQualityLite::Low, 0, "q")])),
                HashMap::from([
                    ("", ("q", 0)),
                    ("q", ("q", 0)),
                    ("h", ("q", 0)),
                    ("f", ("q", 0)),
                ]),
            ),
            (
                Some(track(&[
                    (VideoQualityLite::Low, 0, "q"),
                    (VideoQualityLite::Medium, 1, "h"),
                ])),
                HashMap::from([
                    ("", ("q", 0)),
                    ("q", ("q", 0)),
                    ("h", ("h", 1)),
                    ("f", ("h", 1)),
                ]),
            ),
            (
                Some(track(&[
                    (VideoQualityLite::Low, 0, "q"),
                    (VideoQualityLite::Medium, 1, "h"),
                    (VideoQualityLite::High, 2, "f"),
                ])),
                HashMap::from([
                    ("", ("q", 0)),
                    ("q", ("q", 0)),
                    ("h", ("h", 1)),
                    ("f", ("f", 2)),
                ]),
            ),
        ];

        let rid_space = default_video_layers_rid();
        for (track_info, rid_cases) in tests {
            for (rid, (expected_rid, expected_layer)) in rid_cases {
                let actual_layer = rid_to_spatial_layer(rid, track_info.as_ref(), &rid_space);
                assert_eq!(actual_layer, expected_layer);
                let actual_rid =
                    spatial_layer_to_rid(actual_layer, track_info.as_ref(), &rid_space);
                assert_eq!(actual_rid, expected_rid.to_string());
            }
        }
    }

    #[test]
    fn video_layer_utils_quality_conversion_matches_upstream_contract() {
        let tests: Vec<(
            Option<TrackInfoLite>,
            HashMap<VideoQualityLite, (VideoQualityLite, i32)>,
        )> = vec![
            (
                None,
                HashMap::from([
                    (VideoQualityLite::Low, (VideoQualityLite::Low, 0)),
                    (VideoQualityLite::Medium, (VideoQualityLite::Medium, 1)),
                    (VideoQualityLite::High, (VideoQualityLite::High, 2)),
                ]),
            ),
            (
                Some(track(&[(VideoQualityLite::Low, 0, "q")])),
                HashMap::from([
                    (VideoQualityLite::Low, (VideoQualityLite::Low, 0)),
                    (VideoQualityLite::Medium, (VideoQualityLite::Low, 0)),
                    (VideoQualityLite::High, (VideoQualityLite::Low, 0)),
                ]),
            ),
            (
                Some(track(&[(VideoQualityLite::Medium, 0, "q")])),
                HashMap::from([
                    (VideoQualityLite::Low, (VideoQualityLite::Medium, 0)),
                    (VideoQualityLite::Medium, (VideoQualityLite::Medium, 0)),
                    (VideoQualityLite::High, (VideoQualityLite::Medium, 0)),
                ]),
            ),
            (
                Some(track(&[
                    (VideoQualityLite::Low, 0, "q"),
                    (VideoQualityLite::High, 1, "h"),
                ])),
                HashMap::from([
                    (VideoQualityLite::Low, (VideoQualityLite::Low, 0)),
                    (VideoQualityLite::Medium, (VideoQualityLite::High, 1)),
                    (VideoQualityLite::High, (VideoQualityLite::High, 1)),
                ]),
            ),
            (
                Some(track(&[
                    (VideoQualityLite::Low, 0, "q"),
                    (VideoQualityLite::Medium, 1, "h"),
                    (VideoQualityLite::High, 2, "f"),
                ])),
                HashMap::from([
                    (VideoQualityLite::Low, (VideoQualityLite::Low, 0)),
                    (VideoQualityLite::Medium, (VideoQualityLite::Medium, 1)),
                    (VideoQualityLite::High, (VideoQualityLite::High, 2)),
                ]),
            ),
        ];

        for (track_info, quality_cases) in tests {
            for (quality, (expected_quality, expected_layer)) in quality_cases {
                let layer = video_quality_to_spatial_layer(quality, track_info.as_ref());
                assert_eq!(layer, expected_layer);
                let mapped_quality = spatial_layer_to_video_quality(layer, track_info.as_ref());
                assert_eq!(mapped_quality, expected_quality);
            }
        }
    }

    #[test]
    fn video_layer_utils_video_quality_to_rid_matches_upstream_contract() {
        let rid_space = default_video_layers_rid();
        let test_track = track(&[
            (VideoQualityLite::Low, 0, "q"),
            (VideoQualityLite::Medium, 1, "h"),
            (VideoQualityLite::High, 2, "f"),
        ]);

        assert_eq!(
            video_quality_to_rid(VideoQualityLite::Low, Some(&test_track), &rid_space),
            "q"
        );
        assert_eq!(
            video_quality_to_rid(VideoQualityLite::Medium, Some(&test_track), &rid_space),
            "h"
        );
        assert_eq!(
            video_quality_to_rid(VideoQualityLite::High, Some(&test_track), &rid_space),
            "f"
        );
    }

    #[test]
    fn video_layer_utils_get_spatial_layer_for_rid_matches_upstream_contract() {
        let no_track: Option<TrackInfoLite> = None;
        assert_eq!(
            get_spatial_layer_for_rid("q", no_track.as_ref()),
            INVALID_LAYER_SPATIAL
        );

        let no_rid_layers = track(&[
            (VideoQualityLite::Low, 0, ""),
            (VideoQualityLite::Medium, 1, ""),
        ]);
        assert_eq!(get_spatial_layer_for_rid("q", Some(&no_rid_layers)), 0);

        let with_rids = track(&[
            (VideoQualityLite::Low, 0, "q"),
            (VideoQualityLite::Medium, 1, "h"),
        ]);
        assert_eq!(get_spatial_layer_for_rid("q", Some(&with_rids)), 0);
        assert_eq!(get_spatial_layer_for_rid("h", Some(&with_rids)), 1);
        assert_eq!(get_spatial_layer_for_rid("f", Some(&with_rids)), 0);
    }

    #[test]
    fn video_layer_utils_get_spatial_layer_for_video_quality_matches_upstream_contract() {
        let no_track: Option<TrackInfoLite> = None;
        assert_eq!(
            get_spatial_layer_for_video_quality(VideoQualityLite::Low, no_track.as_ref()),
            INVALID_LAYER_SPATIAL
        );

        let layers = track(&[
            (VideoQualityLite::Low, 0, "q"),
            (VideoQualityLite::Medium, 1, "h"),
        ]);
        assert_eq!(
            get_spatial_layer_for_video_quality(VideoQualityLite::Low, Some(&layers)),
            0
        );
        assert_eq!(
            get_spatial_layer_for_video_quality(VideoQualityLite::Medium, Some(&layers)),
            1
        );
        assert_eq!(
            get_spatial_layer_for_video_quality(VideoQualityLite::High, Some(&layers)),
            1
        );
        assert_eq!(
            get_spatial_layer_for_video_quality(VideoQualityLite::Off, Some(&layers)),
            INVALID_LAYER_SPATIAL
        );
    }

    #[test]
    fn video_layer_utils_get_video_quality_for_spatial_layer_matches_upstream_contract() {
        let no_track: Option<TrackInfoLite> = None;
        assert_eq!(
            get_video_quality_for_spatial_layer(0, no_track.as_ref()),
            VideoQualityLite::Off
        );

        let layers = track(&[
            (VideoQualityLite::Low, 0, "q"),
            (VideoQualityLite::Medium, 1, "h"),
        ]);
        assert_eq!(
            get_video_quality_for_spatial_layer(INVALID_LAYER_SPATIAL, Some(&layers)),
            VideoQualityLite::Off
        );
        assert_eq!(
            get_video_quality_for_spatial_layer(0, Some(&layers)),
            VideoQualityLite::Low
        );
        assert_eq!(
            get_video_quality_for_spatial_layer(1, Some(&layers)),
            VideoQualityLite::Medium
        );
        assert_eq!(
            get_video_quality_for_spatial_layer(2, Some(&layers)),
            VideoQualityLite::Off
        );
    }

    #[test]
    fn quality_for_dimension_matches_upstream_contract() {
        // landscape source
        assert_eq!(
            quality_for_dimension(1080, 720, 120, 120, vec![]),
            VideoQualityLite::Low
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 300, 200, vec![]),
            VideoQualityLite::Low
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 200, 250, vec![]),
            VideoQualityLite::Medium
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 700, 480, vec![]),
            VideoQualityLite::High
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 500, 1000, vec![]),
            VideoQualityLite::High
        );

        // portrait source
        assert_eq!(
            quality_for_dimension(540, 960, 200, 400, vec![]),
            VideoQualityLite::Low
        );
        assert_eq!(
            quality_for_dimension(540, 960, 400, 400, vec![]),
            VideoQualityLite::Medium
        );
        assert_eq!(
            quality_for_dimension(540, 960, 400, 700, vec![]),
            VideoQualityLite::Medium
        );
        assert_eq!(
            quality_for_dimension(540, 960, 600, 900, vec![]),
            VideoQualityLite::High
        );

        // explicit layer sizes (sorted heights: 270, 540, 720)
        let provided = vec![270, 540, 720];
        assert_eq!(
            quality_for_dimension(1080, 720, 120, 120, provided.clone()),
            VideoQualityLite::Low
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 300, 300, provided.clone()),
            VideoQualityLite::Low
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 800, 500, provided.clone()),
            VideoQualityLite::Medium
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 1000, 700, provided.clone()),
            VideoQualityLite::High
        );

        // highest layer with smallest dimensions (duplicate lower/medium)
        let duplicate_low_medium = vec![270, 270, 720];
        assert_eq!(
            quality_for_dimension(1080, 720, 120, 120, duplicate_low_medium.clone()),
            VideoQualityLite::Medium
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 300, 300, duplicate_low_medium.clone()),
            VideoQualityLite::Medium
        );
        assert_eq!(
            quality_for_dimension(1080, 720, 800, 500, duplicate_low_medium.clone()),
            VideoQualityLite::High
        );
    }

    #[test]
    fn video_layer_utils_normalize_video_layers_rid_matches_upstream_contract() {
        let qhf: VideoLayersRid = [String::from("q"), String::from("h"), String::from("f")];
        let two_one_zero: VideoLayersRid =
            [String::from("2"), String::from("1"), String::from("0")];

        assert_eq!(
            normalize_video_layers_rid(&[String::new(), String::new(), String::new()]),
            [String::new(), String::new(), String::new()]
        );
        assert_eq!(
            normalize_video_layers_rid(&[String::from("3"), String::from("2"), String::from("1"),]),
            [String::from("3"), String::from("2"), String::from("1"),]
        );
        assert_eq!(normalize_video_layers_rid(&qhf), qhf);
        assert_eq!(
            normalize_video_layers_rid(&[String::from("f"), String::from("h"), String::from("q"),]),
            [String::from("q"), String::from("h"), String::from("f"),]
        );
        assert_eq!(
            normalize_video_layers_rid(&[String::from("h"), String::from("q"), String::new(),]),
            [String::from("q"), String::from("h"), String::new()]
        );

        assert_eq!(normalize_video_layers_rid(&two_one_zero), two_one_zero);
        assert_eq!(
            normalize_video_layers_rid(&[String::from("2"), String::from("0"), String::from("1"),]),
            [String::from("2"), String::from("1"), String::from("0"),]
        );
        assert_eq!(
            normalize_video_layers_rid(&[String::from("1"), String::from("2"), String::new(),]),
            [String::from("2"), String::from("1"), String::new()]
        );
    }
}
