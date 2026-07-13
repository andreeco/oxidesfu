use livekit_protocol as proto;

use crate::state::SignalState;

pub(crate) fn requested_media_track_sids(request: &proto::UpdateSubscription) -> Vec<String> {
    let mut track_sids = request.track_sids.clone();
    for participant_tracks in &request.participant_tracks {
        track_sids.extend(participant_tracks.track_sids.iter().cloned());
    }
    track_sids.sort();
    track_sids.dedup();
    track_sids
}

pub(crate) fn find_media_track_publisher(
    state: &SignalState,
    room_name: &str,
    track_sid: &str,
) -> Option<(String, proto::TrackInfo)> {
    let participants = state.rooms.list_participants(room_name).ok()?;
    for participant in participants {
        if let Some(track) = participant
            .tracks
            .iter()
            .find(|track| track.sid == track_sid)
            .cloned()
        {
            return Some((participant.identity, track));
        }
    }
    None
}
