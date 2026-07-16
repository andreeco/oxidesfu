#![cfg_attr(not(test), allow(dead_code))]

use std::{
    collections::HashMap,
    sync::{Arc, Mutex},
};

use livekit_protocol as proto;

#[derive(Debug, Default)]
pub(crate) struct ReferenceGuard {
    activated: bool,
    released: bool,
}

#[derive(Debug, Default)]
struct ReferenceCount {
    count: usize,
}

impl ReferenceCount {
    fn activate(&mut self, guard: &mut ReferenceGuard) {
        if !guard.activated {
            guard.activated = true;
            self.count += 1;
        }
    }

    fn release(&mut self, guard: &mut ReferenceGuard) -> bool {
        if !guard.activated || guard.released {
            return false;
        }

        guard.released = true;
        if self.count > 0 {
            self.count -= 1;
        }
        self.count == 0
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct StatsKey {
    stream_type: i32,
    participant_id: String,
    track_id: String,
}

pub(crate) fn stats_key_for_data(
    _country: &str,
    stream_type: proto::StreamType,
    participant_id: &str,
    track_id: &str,
) -> StatsKey {
    StatsKey {
        stream_type: stream_type as i32,
        participant_id: participant_id.to_string(),
        track_id: track_id.to_string(),
    }
}

trait AnalyticsSink: Send + Sync {
    fn send_stats(&self, stats: Vec<proto::AnalyticsStat>);
    fn send_event(&self, event: proto::AnalyticsEvent);
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct WorkerKey {
    room_id: String,
    participant_id: String,
}

#[derive(Debug, Default)]
struct StatsWorker {
    room_id: String,
    room_name: String,
    participant_id: String,
    connected: bool,
    outgoing_per_track: HashMap<String, Vec<proto::AnalyticsStat>>,
    incoming_per_track: HashMap<String, Vec<proto::AnalyticsStat>>,
    ref_count: ReferenceCount,
    closed: bool,
}

impl StatsWorker {
    fn new(
        room_id: &str,
        room_name: &str,
        participant_id: &str,
        guard: Option<&mut ReferenceGuard>,
    ) -> Self {
        let mut worker = Self {
            room_id: room_id.to_string(),
            room_name: room_name.to_string(),
            participant_id: participant_id.to_string(),
            connected: false,
            outgoing_per_track: HashMap::new(),
            incoming_per_track: HashMap::new(),
            ref_count: ReferenceCount::default(),
            closed: false,
        };

        if let Some(guard) = guard {
            worker.ref_count.activate(guard);
        }

        worker
    }

    fn on_track_stat(&mut self, track_id: &str, stream_type: i32, stat: proto::AnalyticsStat) {
        if stream_type == proto::StreamType::Downstream as i32 {
            self.outgoing_per_track
                .entry(track_id.to_string())
                .or_default()
                .push(stat);
        } else {
            self.incoming_per_track
                .entry(track_id.to_string())
                .or_default()
                .push(stat);
        }
    }

    fn set_connected(&mut self) {
        self.connected = true;
    }

    fn close(&mut self, guard: &mut ReferenceGuard) -> bool {
        if !self.ref_count.release(guard) {
            return false;
        }

        let transitioning = !self.closed;
        self.closed = true;
        transitioning
    }

    fn closed(&mut self, guard: Option<&mut ReferenceGuard>) -> bool {
        if self.closed {
            return true;
        }

        if let Some(guard) = guard {
            self.ref_count.activate(guard);
        }

        false
    }

    fn flush(&mut self) -> Vec<proto::AnalyticsStat> {
        let mut out = Vec::new();

        let incoming = std::mem::take(&mut self.incoming_per_track);
        let outgoing = std::mem::take(&mut self.outgoing_per_track);

        self.collect_stats(proto::StreamType::Upstream as i32, incoming, &mut out);
        self.collect_stats(proto::StreamType::Downstream as i32, outgoing, &mut out);

        out
    }

    fn collect_stats(
        &self,
        kind: i32,
        per_track: HashMap<String, Vec<proto::AnalyticsStat>>,
        out: &mut Vec<proto::AnalyticsStat>,
    ) {
        for (track_id, stats) in per_track {
            if let Some(mut coalesced) = coalesce(&stats) {
                coalesced.kind = kind;
                coalesced.room_id = self.room_id.clone();
                coalesced.room_name = self.room_name.clone();
                coalesced.participant_id = self.participant_id.clone();
                coalesced.track_id = track_id;
                out.push(coalesced);
            }
        }
    }
}

fn coalesce(stats: &[proto::AnalyticsStat]) -> Option<proto::AnalyticsStat> {
    if stats.is_empty() {
        return None;
    }

    let mut aggregate = proto::AnalyticsStream::default();
    let mut saw_stream = false;

    for stat in stats {
        for stream in &stat.streams {
            saw_stream = true;
            aggregate.primary_packets = aggregate
                .primary_packets
                .saturating_add(stream.primary_packets);
            aggregate.primary_bytes = aggregate.primary_bytes.saturating_add(stream.primary_bytes);
            aggregate.retransmit_packets = aggregate
                .retransmit_packets
                .saturating_add(stream.retransmit_packets);
            aggregate.retransmit_bytes = aggregate
                .retransmit_bytes
                .saturating_add(stream.retransmit_bytes);
            aggregate.padding_packets = aggregate
                .padding_packets
                .saturating_add(stream.padding_packets);
            aggregate.padding_bytes = aggregate.padding_bytes.saturating_add(stream.padding_bytes);
            aggregate.packets_lost = aggregate.packets_lost.saturating_add(stream.packets_lost);
            aggregate.packets_out_of_order = aggregate
                .packets_out_of_order
                .saturating_add(stream.packets_out_of_order);
            aggregate.frames = aggregate.frames.saturating_add(stream.frames);
            aggregate.nacks = aggregate.nacks.saturating_add(stream.nacks);
            aggregate.plis = aggregate.plis.saturating_add(stream.plis);
            aggregate.firs = aggregate.firs.saturating_add(stream.firs);
            aggregate.rtt = aggregate.rtt.max(stream.rtt);
            aggregate.jitter = aggregate.jitter.max(stream.jitter);

            for layer in &stream.video_layers {
                if let Some(existing) = aggregate
                    .video_layers
                    .iter_mut()
                    .find(|existing| existing.layer == layer.layer)
                {
                    existing.packets = existing.packets.saturating_add(layer.packets);
                    existing.bytes = existing.bytes.saturating_add(layer.bytes);
                    existing.frames = existing.frames.saturating_add(layer.frames);
                } else {
                    aggregate.video_layers.push(layer.clone());
                }
            }
        }
    }

    if !saw_stream {
        return None;
    }

    if let Some(max_layer) = aggregate.video_layers.iter().map(|layer| layer.layer).max()
        && let Some(layer) = aggregate
            .video_layers
            .iter()
            .find(|layer| layer.layer == max_layer)
            .cloned()
    {
        aggregate.video_layers = vec![layer];
    }

    Some(proto::AnalyticsStat {
        streams: vec![aggregate],
        mime: stats
            .last()
            .map(|stat| stat.mime.clone())
            .unwrap_or_default(),
        ..Default::default()
    })
}

pub(crate) struct TelemetryService {
    sink: Arc<dyn AnalyticsSink>,
    workers: Mutex<HashMap<WorkerKey, StatsWorker>>,
}

impl TelemetryService {
    fn new(sink: Arc<dyn AnalyticsSink>) -> Self {
        Self {
            sink,
            workers: Mutex::new(HashMap::new()),
        }
    }

    fn get_or_create_worker<'a>(
        workers: &'a mut HashMap<WorkerKey, StatsWorker>,
        room: &proto::Room,
        participant: &proto::ParticipantInfo,
        guard: Option<&mut ReferenceGuard>,
    ) -> (&'a mut StatsWorker, bool) {
        let key = WorkerKey {
            room_id: room.sid.clone(),
            participant_id: participant.sid.clone(),
        };

        let existed = workers.contains_key(&key);
        let worker = workers
            .entry(key)
            .or_insert_with(|| StatsWorker::new(&room.sid, &room.name, &participant.sid, None));

        if let Some(guard) = guard {
            if worker.closed {
                *worker = StatsWorker::new(&room.sid, &room.name, &participant.sid, Some(guard));
            } else {
                worker.ref_count.activate(guard);
            }
        }

        (worker, existed)
    }

    pub(crate) fn participant_joined(
        &self,
        room: &proto::Room,
        participant: &proto::ParticipantInfo,
        client_info: Option<proto::ClientInfo>,
        client_meta: Option<proto::AnalyticsClientMeta>,
        should_send_event: bool,
        guard: Option<&mut ReferenceGuard>,
    ) {
        {
            let mut workers = self
                .workers
                .lock()
                .expect("workers mutex should not be poisoned");
            let _ = Self::get_or_create_worker(&mut workers, room, participant, guard);
        }

        if should_send_event {
            self.sink.send_event(proto::AnalyticsEvent {
                r#type: proto::AnalyticsEventType::ParticipantJoined as i32,
                room_id: room.sid.clone(),
                room: Some(room.clone()),
                participant_id: participant.sid.clone(),
                participant: Some(participant.clone()),
                client_info,
                client_meta,
                ..Default::default()
            });
        }
    }

    pub(crate) fn participant_active(
        &self,
        room: &proto::Room,
        participant: &proto::ParticipantInfo,
        client_meta: Option<proto::AnalyticsClientMeta>,
        _is_migration: bool,
        guard: Option<&mut ReferenceGuard>,
    ) {
        {
            let mut workers = self
                .workers
                .lock()
                .expect("workers mutex should not be poisoned");
            let (worker, _) = Self::get_or_create_worker(&mut workers, room, participant, guard);
            worker.set_connected();
        }

        self.sink.send_event(proto::AnalyticsEvent {
            r#type: proto::AnalyticsEventType::ParticipantActive as i32,
            room_id: room.sid.clone(),
            room: Some(room.clone()),
            participant_id: participant.sid.clone(),
            participant: Some(participant.clone()),
            client_meta,
            ..Default::default()
        });
    }

    pub(crate) fn participant_left(
        &self,
        room: &proto::Room,
        participant: &proto::ParticipantInfo,
        should_send_event: bool,
        guard: Option<&mut ReferenceGuard>,
    ) {
        if let Some(guard) = guard {
            let mut workers = self
                .workers
                .lock()
                .expect("workers mutex should not be poisoned");
            if let Some(worker) = workers.get_mut(&WorkerKey {
                room_id: room.sid.clone(),
                participant_id: participant.sid.clone(),
            }) {
                let _ = worker.close(guard);
            }
        }

        if should_send_event {
            self.sink.send_event(proto::AnalyticsEvent {
                r#type: proto::AnalyticsEventType::ParticipantLeft as i32,
                room_id: room.sid.clone(),
                room: Some(room.clone()),
                participant_id: participant.sid.clone(),
                participant: Some(participant.clone()),
                ..Default::default()
            });
        }
    }

    pub(crate) fn track_published_update(
        &self,
        room_id: &str,
        room_name: &str,
        participant_id: &str,
        track: &proto::TrackInfo,
    ) {
        self.sink.send_event(proto::AnalyticsEvent {
            r#type: proto::AnalyticsEventType::TrackPublishedUpdate as i32,
            room_id: room_id.to_string(),
            room: Some(proto::Room {
                sid: room_id.to_string(),
                name: room_name.to_string(),
                ..Default::default()
            }),
            participant_id: participant_id.to_string(),
            track_id: track.sid.clone(),
            track: Some(track.clone()),
            ..Default::default()
        });
    }

    pub(crate) fn track_subscribed(
        &self,
        room_id: &str,
        room_name: &str,
        participant_id: &str,
        track: &proto::TrackInfo,
        publisher: &proto::ParticipantInfo,
        should_send_event: bool,
    ) {
        if !should_send_event {
            return;
        }

        self.sink.send_event(proto::AnalyticsEvent {
            r#type: proto::AnalyticsEventType::TrackSubscribed as i32,
            room_id: room_id.to_string(),
            room: Some(proto::Room {
                sid: room_id.to_string(),
                name: room_name.to_string(),
                ..Default::default()
            }),
            participant_id: participant_id.to_string(),
            track_id: track.sid.clone(),
            track: Some(track.clone()),
            publisher: Some(publisher.clone()),
            ..Default::default()
        });
    }

    pub(crate) fn track_unpublished(
        &self,
        _room_id: &str,
        _room_name: &str,
        _participant_id: &str,
        _track: &proto::TrackInfo,
    ) {
        // intentionally no-op for now; stats are flushed only from accumulated track stats.
    }

    pub(crate) fn track_stats(&self, room_id: &str, key: StatsKey, stat: proto::AnalyticsStat) {
        let mut workers = self
            .workers
            .lock()
            .expect("workers mutex should not be poisoned");

        let worker_key = WorkerKey {
            room_id: room_id.to_string(),
            participant_id: key.participant_id,
        };
        if let Some(worker) = workers.get_mut(&worker_key) {
            worker.on_track_stat(&key.track_id, key.stream_type, stat);
        }
    }

    pub(crate) fn flush_stats(&self) {
        let mut all_stats = Vec::new();
        let mut to_remove = Vec::new();

        {
            let mut workers = self
                .workers
                .lock()
                .expect("workers mutex should not be poisoned");
            for (key, worker) in workers.iter_mut() {
                all_stats.extend(worker.flush());
                if worker.closed {
                    to_remove.push(key.clone());
                }
            }
            for key in to_remove {
                workers.remove(&key);
            }
        }

        if all_stats.is_empty() {
            return;
        }

        all_stats.sort_by(|a, b| {
            a.kind
                .cmp(&b.kind)
                .then_with(|| a.track_id.cmp(&b.track_id))
        });

        self.sink.send_stats(all_stats);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct RecordingSink {
        events: Mutex<Vec<proto::AnalyticsEvent>>,
        stats_calls: Mutex<Vec<Vec<proto::AnalyticsStat>>>,
    }

    impl RecordingSink {
        fn send_event_call_count(&self) -> usize {
            self.events
                .lock()
                .expect("events mutex should not be poisoned")
                .len()
        }

        fn send_event_at(&self, index: usize) -> proto::AnalyticsEvent {
            self.events
                .lock()
                .expect("events mutex should not be poisoned")[index]
                .clone()
        }

        fn send_stats_call_count(&self) -> usize {
            self.stats_calls
                .lock()
                .expect("stats_calls mutex should not be poisoned")
                .len()
        }

        fn send_stats_at(&self, index: usize) -> Vec<proto::AnalyticsStat> {
            self.stats_calls
                .lock()
                .expect("stats_calls mutex should not be poisoned")[index]
                .clone()
        }
    }

    impl AnalyticsSink for RecordingSink {
        fn send_stats(&self, stats: Vec<proto::AnalyticsStat>) {
            self.stats_calls
                .lock()
                .expect("stats_calls mutex should not be poisoned")
                .push(stats);
        }

        fn send_event(&self, event: proto::AnalyticsEvent) {
            self.events
                .lock()
                .expect("events mutex should not be poisoned")
                .push(event);
        }
    }

    struct Fixture {
        sut: TelemetryService,
        sink: Arc<RecordingSink>,
    }

    fn create_fixture() -> Fixture {
        let sink = Arc::new(RecordingSink::default());
        let sut = TelemetryService::new(sink.clone());
        Fixture { sut, sink }
    }

    fn room() -> proto::Room {
        proto::Room {
            sid: "RoomSid".to_string(),
            name: "RoomName".to_string(),
            ..Default::default()
        }
    }

    fn participant(sid: &str) -> proto::ParticipantInfo {
        proto::ParticipantInfo {
            sid: sid.to_string(),
            ..Default::default()
        }
    }

    fn stream(bytes: u64, packets: u32) -> proto::AnalyticsStream {
        proto::AnalyticsStream {
            primary_bytes: bytes,
            primary_packets: packets,
            ..Default::default()
        }
    }

    #[test]
    fn on_participant_join_event_is_sent() {
        let fixture = create_fixture();
        let room = room();
        let participant = participant("part1");
        let client_info = proto::ClientInfo {
            sdk: 2,
            version: "v1".to_string(),
            os: "mac".to_string(),
            os_version: "v1".to_string(),
            device_model: "DM1".to_string(),
            browser: "chrome".to_string(),
            browser_version: "97.0.1".to_string(),
            ..Default::default()
        };
        let client_meta = proto::AnalyticsClientMeta {
            region: "dark-side".to_string(),
            node: "moon".to_string(),
            client_addr: "127.0.0.1".to_string(),
            client_connect_time: 420,
            ..Default::default()
        };

        let mut guard = ReferenceGuard::default();
        fixture.sut.participant_joined(
            &room,
            &participant,
            Some(client_info.clone()),
            Some(client_meta.clone()),
            true,
            Some(&mut guard),
        );

        assert_eq!(fixture.sink.send_event_call_count(), 1);
        let event = fixture.sink.send_event_at(0);
        assert_eq!(
            event.r#type,
            proto::AnalyticsEventType::ParticipantJoined as i32
        );
        assert_eq!(event.participant_id, participant.sid);
        assert_eq!(event.room_id, room.sid);
        assert_eq!(event.room.expect("room should exist").name, room.name);

        let sent_client_info = event.client_info.expect("client_info should exist");
        assert_eq!(sent_client_info.sdk, client_info.sdk);
        assert_eq!(sent_client_info.version, client_info.version);

        let sent_client_meta = event.client_meta.expect("client_meta should exist");
        assert_eq!(sent_client_meta.region, client_meta.region);
        assert_eq!(
            sent_client_meta.client_connect_time,
            client_meta.client_connect_time
        );
    }

    #[test]
    fn on_participant_left_event_is_sent() {
        let fixture = create_fixture();
        let room = room();
        let participant = participant("part1");
        let mut guard = ReferenceGuard::default();

        fixture
            .sut
            .participant_active(&room, &participant, None, false, Some(&mut guard));
        fixture
            .sut
            .participant_left(&room, &participant, true, Some(&mut guard));

        assert_eq!(fixture.sink.send_event_call_count(), 2);
        let event = fixture.sink.send_event_at(1);
        assert_eq!(
            event.r#type,
            proto::AnalyticsEventType::ParticipantLeft as i32
        );
        assert_eq!(event.participant_id, participant.sid);
        assert_eq!(event.room_id, room.sid);
    }

    #[test]
    #[allow(deprecated)] // TrackInfo compatibility fixture requires legacy wire fields.
    fn on_track_update_event_is_sent() {
        let fixture = create_fixture();

        let layer = proto::VideoLayer {
            quality: proto::VideoQuality::High as i32,
            width: 360,
            height: 720,
            bitrate: 2048,
            ..Default::default()
        };
        let track = proto::TrackInfo {
            sid: "track1".to_string(),
            r#type: proto::TrackType::Video as i32,
            muted: false,
            simulcast: false,
            disable_dtx: false,
            layers: vec![layer.clone()],
            ..Default::default()
        };

        fixture
            .sut
            .track_published_update("room1", "RoomName", "part1", &track);

        assert_eq!(fixture.sink.send_event_call_count(), 1);
        let event = fixture.sink.send_event_at(0);
        assert_eq!(
            event.r#type,
            proto::AnalyticsEventType::TrackPublishedUpdate as i32
        );
        assert_eq!(event.participant_id, "part1");
        let sent_track = event.track.expect("track should exist");
        assert_eq!(sent_track.sid, "track1");
        assert_eq!(sent_track.layers[0].width, layer.width);
        assert_eq!(sent_track.layers[0].height, layer.height);
        assert_eq!(sent_track.layers[0].quality, layer.quality);
    }

    #[test]
    fn on_participant_active_event_is_sent() {
        let fixture = create_fixture();
        let room = room();
        let participant = participant("part1");
        let mut guard = ReferenceGuard::default();

        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        let client_meta_connect = proto::AnalyticsClientMeta {
            client_connect_time: 420,
            ..Default::default()
        };

        fixture.sut.participant_active(
            &room,
            &participant,
            Some(client_meta_connect.clone()),
            false,
            Some(&mut guard),
        );

        assert_eq!(fixture.sink.send_event_call_count(), 2);
        let event = fixture.sink.send_event_at(1);
        assert_eq!(
            event.r#type,
            proto::AnalyticsEventType::ParticipantActive as i32
        );
        assert_eq!(event.participant_id, participant.sid);
        assert_eq!(event.room_id, room.sid);
        assert_eq!(
            event
                .client_meta
                .expect("client meta should exist")
                .client_connect_time,
            client_meta_connect.client_connect_time
        );
    }

    #[test]
    fn on_track_subscribed_event_is_sent() {
        let fixture = create_fixture();
        let room = room();
        let participant = participant("part1");
        let publisher = proto::ParticipantInfo {
            sid: "pub1".to_string(),
            identity: "publisher".to_string(),
            ..Default::default()
        };
        let track = proto::TrackInfo {
            sid: "tr1".to_string(),
            r#type: proto::TrackType::Video as i32,
            ..Default::default()
        };

        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_subscribed(
            &room.sid,
            &room.name,
            &participant.sid,
            &track,
            &publisher,
            true,
        );

        assert_eq!(fixture.sink.send_event_call_count(), 2);
        let event = fixture.sink.send_event_at(1);
        assert_eq!(
            event.r#type,
            proto::AnalyticsEventType::TrackSubscribed as i32
        );
        assert_eq!(event.participant_id, participant.sid);
        assert_eq!(event.track.expect("track should exist").sid, track.sid);
        let sent_publisher = event.publisher.expect("publisher should exist");
        assert_eq!(sent_publisher.sid, publisher.sid);
        assert_eq!(sent_publisher.identity, publisher.identity);
    }

    #[test]
    fn participant_and_room_data_are_sent_with_analytics() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        let stat = proto::AnalyticsStat {
            streams: vec![stream(33, 0)],
            ..Default::default()
        };
        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, ""),
            stat,
        );

        fixture.sut.flush_stats();

        assert_eq!(fixture.sink.send_stats_call_count(), 1);
        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].kind, proto::StreamType::Downstream as i32);
        assert_eq!(stats[0].participant_id, part_sid);
        assert_eq!(stats[0].room_id, room.sid);
        assert_eq!(stats[0].room_name, room.name);
    }

    #[test]
    fn on_downstream_packets() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        let packets = [33_u64, 23_u64];
        for packet in packets {
            fixture.sut.track_stats(
                &room.sid,
                stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID"),
                proto::AnalyticsStat {
                    streams: vec![proto::AnalyticsStream {
                        primary_bytes: packet,
                        primary_packets: 1,
                        ..Default::default()
                    }],
                    ..Default::default()
                },
            );
        }

        fixture.sut.flush_stats();

        assert_eq!(fixture.sink.send_stats_call_count(), 1);
        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].kind, proto::StreamType::Downstream as i32);
        assert_eq!(stats[0].streams[0].primary_bytes, 56);
        assert_eq!(stats[0].streams[0].primary_packets, 2);
        assert_eq!(stats[0].track_id, "trackID");
    }

    #[test]
    fn on_downstream_packets_several_tracks() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 33,
                    primary_packets: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID2"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 23,
                    primary_packets: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 2);
        let by_track: HashMap<String, proto::AnalyticsStat> = stats
            .into_iter()
            .map(|stat| (stat.track_id.clone(), stat))
            .collect();
        assert_eq!(by_track["trackID1"].streams[0].primary_bytes, 33);
        assert_eq!(by_track["trackID2"].streams[0].primary_bytes, 23);
    }

    #[test]
    fn on_downstream_stat() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 1,
                    primary_packets: 1,
                    packets_lost: 3,
                    nacks: 1,
                    plis: 1,
                    rtt: 23,
                    jitter: 3,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 2,
                    primary_packets: 2,
                    packets_lost: 4,
                    nacks: 1,
                    plis: 1,
                    firs: 1,
                    rtt: 10,
                    jitter: 5,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].kind, proto::StreamType::Downstream as i32);
        assert_eq!(stats[0].streams[0].nacks, 2);
        assert_eq!(stats[0].streams[0].plis, 2);
        assert_eq!(stats[0].streams[0].firs, 1);
        assert_eq!(stats[0].streams[0].rtt, 23);
        assert_eq!(stats[0].streams[0].jitter, 5);
        assert_eq!(stats[0].streams[0].packets_lost, 7);
        assert_eq!(stats[0].track_id, "trackID1");
    }

    #[test]
    fn packet_lost_diff_should_be_sent_to_telemetry() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 1,
                    primary_packets: 1,
                    packets_lost: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 2,
                    primary_packets: 2,
                    packets_lost: 4,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        assert_eq!(fixture.sink.send_stats_call_count(), 2);
        let first = fixture.sink.send_stats_at(0);
        let second = fixture.sink.send_stats_at(1);
        assert_eq!(first[0].streams[0].packets_lost, 1);
        assert_eq!(second[0].streams[0].packets_lost, 4);
    }

    #[test]
    fn on_downstream_rtcp_several_tracks() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![stream(1, 1)],
                ..Default::default()
            },
        );

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 2,
                    primary_packets: 2,
                    nacks: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID2"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 3,
                    primary_packets: 3,
                    firs: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 2);
        let by_track: HashMap<String, proto::AnalyticsStat> = stats
            .into_iter()
            .map(|stat| (stat.track_id.clone(), stat))
            .collect();
        assert_eq!(
            by_track["trackID1"].kind,
            proto::StreamType::Downstream as i32
        );
        assert_eq!(by_track["trackID1"].streams[0].nacks, 1);
        assert_eq!(
            by_track["trackID2"].kind,
            proto::StreamType::Downstream as i32
        );
        assert_eq!(by_track["trackID2"].streams[0].firs, 1);
    }

    #[test]
    fn on_upstream_stat() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 1,
                    primary_packets: 1,
                    packets_lost: 3,
                    nacks: 1,
                    plis: 1,
                    firs: 1,
                    rtt: 13,
                    jitter: 5,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 2,
                    primary_packets: 2,
                    packets_lost: 4,
                    nacks: 1,
                    plis: 1,
                    firs: 1,
                    rtt: 33,
                    jitter: 2,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].kind, proto::StreamType::Upstream as i32);
        assert_eq!(stats[0].streams[0].nacks, 2);
        assert_eq!(stats[0].streams[0].plis, 2);
        assert_eq!(stats[0].streams[0].firs, 2);
        assert_eq!(stats[0].streams[0].rtt, 33);
        assert_eq!(stats[0].streams[0].jitter, 5);
        assert_eq!(stats[0].streams[0].packets_lost, 7);
    }

    #[test]
    fn on_upstream_rtcp_several_tracks() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = proto::ParticipantInfo {
            sid: part_sid.to_string(),
            identity: "part1Identity".to_string(),
            ..Default::default()
        };
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        let baseline = proto::AnalyticsStat {
            streams: vec![stream(1, 1)],
            ..Default::default()
        };
        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID1"),
            baseline.clone(),
        );
        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID2"),
            baseline,
        );

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 2,
                    primary_packets: 2,
                    nacks: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );
        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID2"),
            proto::AnalyticsStat {
                streams: vec![proto::AnalyticsStream {
                    primary_bytes: 2,
                    primary_packets: 2,
                    firs: 1,
                    ..Default::default()
                }],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 2);
        for stat in &stats {
            assert_eq!(stat.streams[0].primary_bytes, 3);
            assert_eq!(stat.streams[0].primary_packets, 3);
        }

        fixture.sut.track_unpublished(
            &room.sid,
            &room.name,
            part_sid,
            &proto::TrackInfo {
                sid: "trackID2".to_string(),
                ..Default::default()
            },
        );
        fixture.sut.flush_stats();
        assert_eq!(fixture.sink.send_stats_call_count(), 1);
    }

    #[test]
    fn analytics_sent_when_participant_leaves() {
        let fixture = create_fixture();
        let room = room();
        let participant = participant("part1");
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture
            .sut
            .participant_left(&room, &participant, true, Some(&mut guard));

        assert_eq!(fixture.sink.send_stats_call_count(), 0);
    }

    #[test]
    fn add_up_track() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID"),
            proto::AnalyticsStat {
                streams: vec![stream(3, 3)],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].kind, proto::StreamType::Upstream as i32);
        assert_eq!(stats[0].streams[0].primary_bytes, 3);
        assert_eq!(stats[0].streams[0].primary_packets, 3);
        assert_eq!(stats[0].track_id, "trackID");
    }

    #[test]
    fn add_up_track_several_buffers_simulcast() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID"),
            proto::AnalyticsStat {
                streams: vec![stream(1, 1), stream(2, 2)],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 1);
        assert_eq!(stats[0].kind, proto::StreamType::Upstream as i32);
        assert_eq!(stats[0].streams[0].primary_bytes, 3);
        assert_eq!(stats[0].streams[0].primary_packets, 3);
        assert_eq!(stats[0].track_id, "trackID");
    }

    #[test]
    fn both_downstream_and_upstream_stats_are_sent_together() {
        let fixture = create_fixture();
        let room = room();
        let part_sid = "part1";
        let participant = participant(part_sid);
        let mut guard = ReferenceGuard::default();
        fixture
            .sut
            .participant_joined(&room, &participant, None, None, true, Some(&mut guard));

        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Upstream, part_sid, "trackID"),
            proto::AnalyticsStat {
                streams: vec![stream(3, 3)],
                ..Default::default()
            },
        );
        fixture.sut.track_stats(
            &room.sid,
            stats_key_for_data("test", proto::StreamType::Downstream, part_sid, "trackID1"),
            proto::AnalyticsStat {
                streams: vec![stream(1, 1)],
                ..Default::default()
            },
        );

        fixture.sut.flush_stats();

        let stats = fixture.sink.send_stats_at(0);
        assert_eq!(stats.len(), 2);
        assert_eq!(stats[0].kind, proto::StreamType::Upstream as i32);
        assert_eq!(stats[1].kind, proto::StreamType::Downstream as i32);
    }

    #[test]
    fn stats_worker_reference_counted_close_works() {
        let mut g0 = ReferenceGuard::default();
        let mut g1 = ReferenceGuard::default();
        let mut worker = StatsWorker::new("room", "roomName", "participant", Some(&mut g0));

        assert!(!worker.closed(Some(&mut g1)));
        assert!(!worker.close(&mut g0));
        assert!(!worker.closed(Some(&mut g1)));
        assert!(worker.close(&mut g1));
        assert!(worker.closed(Some(&mut g1)));
    }
}
