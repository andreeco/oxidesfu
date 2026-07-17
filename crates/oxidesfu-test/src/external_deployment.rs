use super::*;

#[tokio::test]
#[ignore = "requires LIVEKIT_URL, LIVEKIT_API_KEY, and LIVEKIT_API_SECRET for an external deployment"]
async fn external_deployment_audio_roundtrip() {
    let _ = rustls::crypto::aws_lc_rs::default_provider().install_default();

    let server_url = std::env::var("LIVEKIT_URL")
        .expect("LIVEKIT_URL must point to the external deployment")
        .replacen("wss://", "https://", 1)
        .replacen("ws://", "http://", 1);
    let api_key = std::env::var("LIVEKIT_API_KEY").expect("LIVEKIT_API_KEY must be set");
    let api_secret = std::env::var("LIVEKIT_API_SECRET").expect("LIVEKIT_API_SECRET must be set");
    let suffix = unique_suffix();
    let room_name = format!("external-audio-roundtrip-{suffix}");
    let publisher_identity = format!("external-audio-publisher-{suffix}");
    let subscriber_identity = format!("external-audio-subscriber-{suffix}");

    let publisher_token = AccessToken::with_api_key(&api_key, &api_secret)
        .with_identity(&publisher_identity)
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let subscriber_token = AccessToken::with_api_key(&api_key, &api_secret)
        .with_identity(&subscriber_identity)
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name,
            can_publish: false,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = true;
    options.connect_timeout = Duration::from_secs(20);

    let (publisher_room, mut publisher_events) =
        Room::connect(&server_url, &publisher_token, options.clone())
            .await
            .expect("publisher should connect to the external deployment");
    let (subscriber_room, mut subscriber_events) =
        Room::connect(&server_url, &subscriber_token, options)
            .await
            .expect("subscriber should connect to the external deployment");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut subscriber_events).await;

    let source = NativeAudioSource::new(AudioSourceOptions::default(), 48_000, 1, 1_000);
    let track = LocalAudioTrack::create_audio_track(
        "external-audio-roundtrip",
        RtcAudioSource::Native(source.clone()),
    );
    let publication = publisher_room
        .local_participant()
        .publish_track(LocalTrack::Audio(track), TrackPublishOptions::default())
        .await
        .expect("publisher should publish audio");
    let published_sid = publication.sid().to_string();
    let frame = AudioFrame {
        data: vec![555_i16; 480].into(),
        sample_rate: 48_000,
        num_channels: 1,
        samples_per_channel: 480,
    };

    let remote_audio_track = tokio::time::timeout(Duration::from_secs(20), async {
        loop {
            source
                .capture_frame(&frame)
                .await
                .expect("audio frame should be accepted while awaiting subscription");
            let Ok(Some(event)) =
                tokio::time::timeout(Duration::from_millis(200), subscriber_events.recv()).await
            else {
                continue;
            };
            if let RoomEvent::TrackSubscribed {
                track, publication, ..
            } = event
                && publication.sid().to_string() == published_sid
                && let livekit::track::RemoteTrack::Audio(audio_track) = track
            {
                break audio_track;
            }
        }
    })
    .await
    .expect("subscriber should receive an audio subscription");

    let mut stream = NativeAudioStream::new(remote_audio_track.rtc_track(), 48_000, 1);
    let mut received_audio = false;
    for _ in 0..100 {
        source
            .capture_frame(&frame)
            .await
            .expect("audio frame should be accepted while awaiting delivery");
        if let Ok(Some(_)) = tokio::time::timeout(Duration::from_millis(200), stream.next()).await {
            received_audio = true;
            break;
        }
    }

    let _ = publisher_room.close().await;
    let _ = subscriber_room.close().await;
    assert!(received_audio, "subscriber should receive an audio frame");
}
