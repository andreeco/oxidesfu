use super::*;

// Native SDK media probes create real local peer connections, encoders, and video streams.
// Run those probes one at a time so cadence assertions measure forwarding policy rather than
// contention from another in-process media topology.
static NATIVE_MEDIA_PROBE_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

async fn wait_for_remote_video_subscription(
    events: &mut tokio::sync::mpsc::UnboundedReceiver<RoomEvent>,
) -> (
    livekit::track::RemoteVideoTrack,
    livekit::publication::RemoteTrackPublication,
) {
    tokio::time::timeout(Duration::from_secs(12), async {
        loop {
            let event = events
                .recv()
                .await
                .expect("subscriber room events should stay open");
            if let RoomEvent::TrackSubscribed {
                track, publication, ..
            } = event
                && let livekit::track::RemoteTrack::Video(video_track) = track
            {
                break (video_track, publication);
            }
        }
    })
    .await
    .expect("subscriber should receive remote video TrackSubscribed event before timeout")
}

async fn collect_video_frame_pixels(
    stream: &mut livekit::webrtc::video_stream::native::NativeVideoStream,
    sample_count: usize,
    timeout_per_sample: Duration,
) -> Vec<u32> {
    let mut pixels = Vec::with_capacity(sample_count);
    for _ in 0..sample_count {
        let frame = tokio::time::timeout(timeout_per_sample, stream.next())
            .await
            .expect("video frame wait should finish")
            .expect("video stream should yield a frame while publisher is active");
        let width = frame.buffer.width();
        let height = frame.buffer.height();
        pixels.push(width.saturating_mul(height));
    }
    pixels
}

async fn count_video_frames_for_duration(
    stream: &mut livekit::webrtc::video_stream::native::NativeVideoStream,
    duration: Duration,
    timeout_per_frame: Duration,
) -> usize {
    let deadline = tokio::time::Instant::now() + duration;
    let mut frames = 0usize;
    while tokio::time::Instant::now() < deadline {
        if let Ok(Some(_frame)) = tokio::time::timeout(timeout_per_frame, stream.next()).await {
            frames = frames.saturating_add(1);
        }
    }
    frames
}

async fn drain_video_frames_for_duration(
    stream: &mut livekit::webrtc::video_stream::native::NativeVideoStream,
    duration: Duration,
) {
    let deadline = tokio::time::Instant::now() + duration;
    while tokio::time::Instant::now() < deadline {
        let _ = tokio::time::timeout(Duration::from_millis(25), stream.next()).await;
    }
}

#[tokio::test]
async fn rust_sdk_room_simulcast_video_fps_setting_reduces_observed_frame_cadence_contract() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, oxidesfu_server::app())
            .await
            .expect("test server should run");
    });

    let room_name = format!("sdk-video-fps-contract-{}", unique_suffix());
    let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-fps-publisher")
        .with_name("SDK Video FPS Publisher")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-fps-subscriber")
        .with_name("SDK Video FPS Subscriber")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name,
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);

    let (publisher_room, mut publisher_events) =
        Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
            .await
            .expect("publisher room should connect");
    let (subscriber_room, mut subscriber_events) =
        Room::connect(&format!("http://{addr}"), &subscriber_token, options)
            .await
            .expect("subscriber room should connect");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut subscriber_events).await;

    let source = NativeVideoSource::new(
        VideoResolution {
            width: 1280,
            height: 720,
        },
        false,
    );
    let track = LocalVideoTrack::create_video_track("cam", RtcVideoSource::Native(source.clone()));
    let _publication = publisher_room
        .local_participant()
        .publish_track(
            LocalTrack::Video(track),
            TrackPublishOptions {
                simulcast: true,
                ..Default::default()
            },
        )
        .await
        .expect("publisher should publish simulcast video track");

    let (remote_video_track, remote_publication) =
        wait_for_remote_video_subscription(&mut subscriber_events).await;
    assert!(
        remote_publication.simulcasted(),
        "remote publication should be simulcasted for FPS contract"
    );

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let frame_pump_source = source.clone();
    let frame_pump = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(33));
        let mut luma: u8 = 96;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    let mut buffer = livekit::webrtc::prelude::I420Buffer::new(1280, 720);
                    let (y, u, v) = buffer.data_mut();
                    y.fill(luma);
                    u.fill(128);
                    v.fill(128);
                    luma = luma.wrapping_add(1);
                    let frame = livekit::webrtc::prelude::VideoFrame::new(
                        livekit::webrtc::prelude::VideoRotation::VideoRotation0,
                        buffer,
                    );
                    frame_pump_source.capture_frame(&frame);
                }
            }
        }
    });

    let mut video_stream =
        livekit::webrtc::video_stream::native::NativeVideoStream::new(remote_video_track.rtc_track());

    remote_publication.set_video_quality(livekit::track::VideoQuality::High);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let baseline_frames = count_video_frames_for_duration(
        &mut video_stream,
        Duration::from_secs(4),
        Duration::from_millis(400),
    )
    .await;

    remote_publication.set_video_fps(8);
    tokio::time::sleep(Duration::from_secs(1)).await;
    drain_video_frames_for_duration(&mut video_stream, Duration::from_secs(1)).await;

    let throttled_frames = count_video_frames_for_duration(
        &mut video_stream,
        Duration::from_secs(4),
        Duration::from_millis(500),
    )
    .await;

    assert!(
        baseline_frames >= 20,
        "baseline frame cadence should deliver enough frames, got {}",
        baseline_frames
    );
    assert!(
        throttled_frames > 0,
        "throttled FPS mode should still deliver decodable frames"
    );
    assert!(
        throttled_frames.saturating_mul(100) <= baseline_frames.saturating_mul(75),
        "FPS layer selection should materially reduce frame cadence (baseline={}, throttled={})",
        baseline_frames,
        throttled_frames
    );

    let _ = stop_tx.send(());
    let _ = frame_pump.await;

    let _ = publisher_room.close().await;
    let _ = subscriber_room.close().await;
    server.abort();
}

#[tokio::test]
async fn rust_sdk_room_av1_dd_video_fps_setting_reduces_observed_frame_cadence_contract() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, oxidesfu_server::app())
            .await
            .expect("test server should run");
    });

    let room_name = format!("sdk-av1-dd-fps-contract-{}", unique_suffix());
    let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-av1-dd-fps-publisher")
        .with_name("SDK AV1 DD FPS Publisher")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-av1-dd-fps-subscriber")
        .with_name("SDK AV1 DD FPS Subscriber")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name,
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);

    let (publisher_room, mut publisher_events) =
        Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
            .await
            .expect("publisher room should connect");
    let (subscriber_room, mut subscriber_events) =
        Room::connect(&format!("http://{addr}"), &subscriber_token, options)
            .await
            .expect("subscriber room should connect");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut subscriber_events).await;

    let source = NativeVideoSource::new(
        VideoResolution {
            width: 1280,
            height: 720,
        },
        false,
    );
    let track = LocalVideoTrack::create_video_track("cam", RtcVideoSource::Native(source.clone()));
    let publication = publisher_room
        .local_participant()
        .publish_track(
            LocalTrack::Video(track),
            TrackPublishOptions {
                simulcast: false,
                video_codec: livekit::options::VideoCodec::AV1,
                scalability_mode: Some("L3T3_KEY".to_string()),
                ..Default::default()
            },
        )
        .await;
    if let Err(err) = publication {
        eprintln!(
            "skipping av1/dd fps contract because AV1 publish is unavailable in this environment: {err:?}"
        );
        let _ = publisher_room.close().await;
        let _ = subscriber_room.close().await;
        server.abort();
        return;
    }

    let (remote_video_track, remote_publication) =
        wait_for_remote_video_subscription(&mut subscriber_events).await;

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let frame_pump_source = source.clone();
    let frame_pump = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(33));
        let mut luma: u8 = 96;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    let mut buffer = livekit::webrtc::prelude::I420Buffer::new(1280, 720);
                    let (y, u, v) = buffer.data_mut();
                    y.fill(luma);
                    u.fill(128);
                    v.fill(128);
                    luma = luma.wrapping_add(1);
                    let frame = livekit::webrtc::prelude::VideoFrame::new(
                        livekit::webrtc::prelude::VideoRotation::VideoRotation0,
                        buffer,
                    );
                    frame_pump_source.capture_frame(&frame);
                }
            }
        }
    });

    let mut video_stream =
        livekit::webrtc::video_stream::native::NativeVideoStream::new(remote_video_track.rtc_track());

    tokio::time::sleep(Duration::from_secs(1)).await;
    let baseline_frames = count_video_frames_for_duration(
        &mut video_stream,
        Duration::from_secs(4),
        Duration::from_millis(450),
    )
    .await;

    // DD/SVC path: low requested FPS should lower effective forwarded cadence.
    remote_publication.set_video_fps(8);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let throttled_frames = count_video_frames_for_duration(
        &mut video_stream,
        Duration::from_secs(4),
        Duration::from_millis(650),
    )
    .await;

    if baseline_frames == 0 {
        // AV1 encode/decode availability is environment-dependent (driver/runtime/CPU capabilities).
        // When baseline AV1 frames are not decodable here, keep this contract non-flaky by reporting
        // an explicit skip instead of failing a DD/FPS assertion on missing media support.
        eprintln!(
            "skipping av1/dd fps cadence assertions because no baseline AV1 frames were decoded in this environment"
        );
        let _ = stop_tx.send(());
        let _ = frame_pump.await;
        let _ = publisher_room.close().await;
        let _ = subscriber_room.close().await;
        server.abort();
        return;
    }

    assert!(
        baseline_frames >= 16,
        "baseline AV1/DD cadence should deliver enough frames, got {}",
        baseline_frames
    );
    assert!(
        throttled_frames > 0,
        "throttled AV1/DD mode should still deliver decodable frames"
    );
    assert!(
        throttled_frames.saturating_mul(100) <= baseline_frames.saturating_mul(75),
        "AV1/DD FPS throttling should materially reduce frame cadence (baseline={}, throttled={})",
        baseline_frames,
        throttled_frames
    );

    let _ = stop_tx.send(());
    let _ = frame_pump.await;

    let _ = publisher_room.close().await;
    let _ = subscriber_room.close().await;
    server.abort();
}

#[tokio::test]
async fn rust_sdk_room_simulcast_video_fps_isolated_per_subscriber_contract() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, oxidesfu_server::app())
            .await
            .expect("test server should run");
    });

    let room_name = format!("sdk-video-fps-isolated-{}", unique_suffix());
    let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-fps-isolated-publisher")
        .with_name("SDK Video FPS Isolated Publisher")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let low_subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-fps-isolated-low-sub")
        .with_name("SDK Video FPS Isolated Low Subscriber")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("low subscriber token should encode");
    let high_subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-fps-isolated-high-sub")
        .with_name("SDK Video FPS Isolated High Subscriber")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name,
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("high subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);

    let (publisher_room, mut publisher_events) =
        Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
            .await
            .expect("publisher room should connect");
    let (low_subscriber_room, mut low_subscriber_events) =
        Room::connect(
            &format!("http://{addr}"),
            &low_subscriber_token,
            options.clone(),
        )
        .await
        .expect("low subscriber room should connect");
    let (high_subscriber_room, mut high_subscriber_events) =
        Room::connect(&format!("http://{addr}"), &high_subscriber_token, options)
            .await
            .expect("high subscriber room should connect");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut low_subscriber_events).await;
    wait_for_room_connected(&mut high_subscriber_events).await;

    let source = NativeVideoSource::new(
        VideoResolution {
            width: 1280,
            height: 720,
        },
        false,
    );
    let track = LocalVideoTrack::create_video_track("cam", RtcVideoSource::Native(source.clone()));
    let _publication = publisher_room
        .local_participant()
        .publish_track(
            LocalTrack::Video(track),
            TrackPublishOptions {
                simulcast: true,
                ..Default::default()
            },
        )
        .await
        .expect("publisher should publish simulcast video track");

    let (low_remote_video_track, low_remote_publication) =
        wait_for_remote_video_subscription(&mut low_subscriber_events).await;
    let (high_remote_video_track, high_remote_publication) =
        wait_for_remote_video_subscription(&mut high_subscriber_events).await;
    assert!(
        low_remote_publication.simulcasted() && high_remote_publication.simulcasted(),
        "remote publications should be simulcasted for fps isolation contract"
    );

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let frame_pump_source = source.clone();
    let frame_pump = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(33));
        let mut luma: u8 = 96;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    let mut buffer = livekit::webrtc::prelude::I420Buffer::new(1280, 720);
                    let (y, u, v) = buffer.data_mut();
                    y.fill(luma);
                    u.fill(128);
                    v.fill(128);
                    luma = luma.wrapping_add(1);
                    let frame = livekit::webrtc::prelude::VideoFrame::new(
                        livekit::webrtc::prelude::VideoRotation::VideoRotation0,
                        buffer,
                    );
                    frame_pump_source.capture_frame(&frame);
                }
            }
        }
    });

    let mut low_stream =
        livekit::webrtc::video_stream::native::NativeVideoStream::new(low_remote_video_track.rtc_track());
    let mut high_stream =
        livekit::webrtc::video_stream::native::NativeVideoStream::new(high_remote_video_track.rtc_track());

    low_remote_publication.set_video_quality(livekit::track::VideoQuality::High);
    high_remote_publication.set_video_quality(livekit::track::VideoQuality::High);
    tokio::time::sleep(Duration::from_secs(1)).await;

    let low_baseline = count_video_frames_for_duration(
        &mut low_stream,
        Duration::from_secs(3),
        Duration::from_millis(450),
    );
    let high_baseline = count_video_frames_for_duration(
        &mut high_stream,
        Duration::from_secs(3),
        Duration::from_millis(450),
    );
    let (low_baseline, high_baseline) = tokio::join!(low_baseline, high_baseline);

    low_remote_publication.set_video_fps(6);
    high_remote_publication.set_video_fps(30);
    tokio::time::sleep(Duration::from_millis(1200)).await;

    let low_measured = count_video_frames_for_duration(
        &mut low_stream,
        Duration::from_secs(5),
        Duration::from_millis(600),
    );
    let high_measured = count_video_frames_for_duration(
        &mut high_stream,
        Duration::from_secs(5),
        Duration::from_millis(400),
    );
    let (low_measured, high_measured) = tokio::join!(low_measured, high_measured);

    assert!(
        low_baseline >= 12 && high_baseline >= 12,
        "both subscribers should receive baseline video before fps split (low_baseline={}, high_baseline={})",
        low_baseline,
        high_baseline
    );
    assert!(
        low_measured > 0,
        "low-fps subscriber should still receive decodable frames"
    );
    assert!(
        high_measured >= 20,
        "high-fps subscriber should continue receiving high cadence frames (high_measured={})",
        high_measured
    );
    assert!(
        low_measured.saturating_mul(100) <= high_measured.saturating_mul(75),
        "low-fps subscriber should observe materially lower cadence than high-fps subscriber (low_measured={}, high_measured={})",
        low_measured,
        high_measured
    );

    let _ = stop_tx.send(());
    let _ = frame_pump.await;

    let _ = publisher_room.close().await;
    let _ = low_subscriber_room.close().await;
    let _ = high_subscriber_room.close().await;
    server.abort();
}

#[tokio::test]
async fn rust_sdk_room_simulcast_video_quality_isolated_per_subscriber_contract() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, oxidesfu_server::app())
            .await
            .expect("test server should run");
    });

    let room_name = format!("sdk-video-quality-isolated-{}", unique_suffix());
    let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-quality-isolated-publisher")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let low_subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-quality-isolated-low")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("low subscriber token should encode");
    let high_subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-quality-isolated-high")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name,
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("high subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);
    let (publisher_room, mut publisher_events) =
        Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
            .await
            .expect("publisher room should connect");
    let (low_subscriber_room, mut low_events) =
        Room::connect(&format!("http://{addr}"), &low_subscriber_token, options.clone())
            .await
            .expect("low subscriber room should connect");
    let (high_subscriber_room, mut high_events) =
        Room::connect(&format!("http://{addr}"), &high_subscriber_token, options)
            .await
            .expect("high subscriber room should connect");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut low_events).await;
    wait_for_room_connected(&mut high_events).await;

    let source = NativeVideoSource::new(
        VideoResolution {
            width: 1280,
            height: 720,
        },
        false,
    );
    let track = LocalVideoTrack::create_video_track("cam", RtcVideoSource::Native(source.clone()));
    let _publication = publisher_room
        .local_participant()
        .publish_track(
            LocalTrack::Video(track),
            TrackPublishOptions {
                simulcast: true,
                ..Default::default()
            },
        )
        .await
        .expect("publisher should publish simulcast video track");

    let (low_track, low_publication) = wait_for_remote_video_subscription(&mut low_events).await;
    let (high_track, high_publication) = wait_for_remote_video_subscription(&mut high_events).await;
    assert!(
        low_publication.simulcasted() && high_publication.simulcasted(),
        "both remote publications should advertise simulcast"
    );

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let frame_pump_source = source.clone();
    let frame_pump = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(33));
        let mut luma: u8 = 96;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    let mut buffer = livekit::webrtc::prelude::I420Buffer::new(1280, 720);
                    let (y, u, v) = buffer.data_mut();
                    y.fill(luma);
                    u.fill(128);
                    v.fill(128);
                    luma = luma.wrapping_add(1);
                    let frame = livekit::webrtc::prelude::VideoFrame::new(
                        livekit::webrtc::prelude::VideoRotation::VideoRotation0,
                        buffer,
                    );
                    frame_pump_source.capture_frame(&frame);
                }
            }
        }
    });

    let mut low_stream = livekit::webrtc::video_stream::native::NativeVideoStream::new(low_track.rtc_track());
    let mut high_stream = livekit::webrtc::video_stream::native::NativeVideoStream::new(high_track.rtc_track());

    low_publication.set_video_quality(livekit::track::VideoQuality::Low);
    high_publication.set_video_quality(livekit::track::VideoQuality::High);
    tokio::time::sleep(Duration::from_secs(1)).await;
    tokio::join!(
        drain_video_frames_for_duration(&mut low_stream, Duration::from_secs(1)),
        drain_video_frames_for_duration(&mut high_stream, Duration::from_secs(1)),
    );

    let (low_pixels, high_pixels) = tokio::join!(
        collect_video_frame_pixels(&mut low_stream, 12, Duration::from_secs(3)),
        collect_video_frame_pixels(&mut high_stream, 12, Duration::from_secs(3)),
    );
    let low_max = low_pixels.iter().copied().max().unwrap_or_default();
    let high_min = high_pixels.iter().copied().min().unwrap_or_default();
    assert!(low_max > 0 && high_min > 0, "both targets should decode video");
    assert!(
        low_max < high_min,
        "simultaneous low/high targets must remain spatially isolated (low_max={low_max}, high_min={high_min})"
    );

    // Upgrading only the low target must converge to high without resetting the other target.
    low_publication.set_video_quality(livekit::track::VideoQuality::High);
    tokio::time::sleep(Duration::from_secs(1)).await;
    tokio::join!(
        drain_video_frames_for_duration(&mut low_stream, Duration::from_secs(1)),
        drain_video_frames_for_duration(&mut high_stream, Duration::from_secs(1)),
    );
    let (upgraded_low_pixels, stable_high_pixels) = tokio::join!(
        collect_video_frame_pixels(&mut low_stream, 12, Duration::from_secs(3)),
        collect_video_frame_pixels(&mut high_stream, 12, Duration::from_secs(3)),
    );
    let upgraded_low_min = upgraded_low_pixels.iter().copied().min().unwrap_or_default();
    let stable_high_min = stable_high_pixels.iter().copied().min().unwrap_or_default();
    assert!(
        upgraded_low_min > low_max,
        "the upgraded target must recover high dimensions (before={low_max}, after={upgraded_low_min})"
    );
    assert!(
        stable_high_min >= high_min,
        "updating one target must not lower the other target (before={high_min}, after={stable_high_min})"
    );

    let _ = stop_tx.send(());
    let _ = frame_pump.await;
    let _ = publisher_room.close().await;
    let _ = low_subscriber_room.close().await;
    let _ = high_subscriber_room.close().await;
    server.abort();
}

#[tokio::test]
async fn rust_sdk_room_allocation_drives_simulcast_spatial_and_temporal_downgrade_upgrade_contract() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have a local address");

    let config = oxidesfu_core::ServerConfig::development();
    let api_state = oxidesfu_server::api_state_from_config(&config);
    let signal_state = oxidesfu_signaling::SignalState::with_data_channels(
        api_state.rooms.clone(),
        api_state.auth.clone(),
        api_state.data_channels.clone(),
    );
    let room_name = format!("sdk-allocation-transition-{}", unique_suffix());
    const SUBSCRIBER_IDENTITY: &str = "sdk-allocation-subscriber";
    signal_state.set_test_support_available_outgoing_bitrate_bps(
        &room_name,
        SUBSCRIBER_IDENTITY,
        Some(2_000_000),
    );
    let app = oxidesfu_server::app_with_api_signal_state_and_readiness(
        api_state,
        signal_state.clone(),
        None,
        Arc::new(oxidesfu_server::AlwaysReadyRelayBackendReadiness),
    );
    let server = tokio::spawn(async move {
        axum::serve(listener, app)
            .await
            .expect("test server should run");
    });

    let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-allocation-publisher")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity(SUBSCRIBER_IDENTITY)
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);
    let (publisher_room, mut publisher_events) =
        Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
            .await
            .expect("publisher room should connect");
    let (subscriber_room, mut subscriber_events) =
        Room::connect(&format!("http://{addr}"), &subscriber_token, options)
            .await
            .expect("subscriber room should connect");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut subscriber_events).await;

    let source = NativeVideoSource::new(
        VideoResolution {
            width: 1280,
            height: 720,
        },
        false,
    );
    let track = LocalVideoTrack::create_video_track("cam", RtcVideoSource::Native(source.clone()));
    let _publication = publisher_room
        .local_participant()
        .publish_track(
            LocalTrack::Video(track),
            TrackPublishOptions {
                simulcast: true,
                video_encoding: Some(livekit::options::VideoEncoding {
                    max_bitrate: 1_000_000,
                    max_framerate: 30.0,
                }),
                simulcast_layers: Some(vec![
                    livekit::options::VideoPreset::new(320, 180, 150_000, 8.0),
                    livekit::options::VideoPreset::new(640, 360, 400_000, 30.0),
                ]),
                ..Default::default()
            },
        )
        .await
        .expect("publisher should publish a simulcast video track");
    let (remote_video_track, remote_publication) =
        wait_for_remote_video_subscription(&mut subscriber_events).await;
    assert!(
        remote_publication.simulcasted(),
        "remote publication must advertise simulcast for allocation coverage"
    );

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let frame_pump_source = source.clone();
    let frame_pump = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(33));
        let mut luma = 96_u8;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    let mut buffer = livekit::webrtc::prelude::I420Buffer::new(1280, 720);
                    let (y, u, v) = buffer.data_mut();
                    y.fill(luma);
                    u.fill(128);
                    v.fill(128);
                    luma = luma.wrapping_add(1);
                    let frame = livekit::webrtc::prelude::VideoFrame::new(
                        livekit::webrtc::prelude::VideoRotation::VideoRotation0,
                        buffer,
                    );
                    frame_pump_source.capture_frame(&frame);
                }
            }
        }
    });
    let mut video_stream =
        livekit::webrtc::video_stream::native::NativeVideoStream::new(remote_video_track.rtc_track());

    tokio::time::sleep(Duration::from_secs(2)).await;
    drain_video_frames_for_duration(&mut video_stream, Duration::from_secs(1)).await;
    let high_pixels = collect_video_frame_pixels(&mut video_stream, 12, Duration::from_secs(3)).await;
    let high_frames = count_video_frames_for_duration(
        &mut video_stream,
        Duration::from_secs(3),
        Duration::from_millis(400),
    )
    .await;
    let high_min_pixels = high_pixels.iter().copied().min().unwrap_or_default();
    assert!(
        high_min_pixels >= 640 * 360,
        "the high allocation should deliver high-resolution frames before downgrade (min_pixels={high_min_pixels})"
    );
    assert!(
        high_frames >= 18,
        "the high allocation should deliver a sustained cadence before downgrade (frames={high_frames})"
    );

    signal_state.set_test_support_available_outgoing_bitrate_bps(
        &room_name,
        SUBSCRIBER_IDENTITY,
        Some(100_000),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    drain_video_frames_for_duration(&mut video_stream, Duration::from_secs(1)).await;
    let low_pixels = collect_video_frame_pixels(&mut video_stream, 12, Duration::from_secs(3)).await;
    let low_frames = count_video_frames_for_duration(
        &mut video_stream,
        Duration::from_secs(3),
        Duration::from_millis(650),
    )
    .await;
    let low_max_pixels = low_pixels.iter().copied().max().unwrap_or_default();
    assert!(low_max_pixels > 0, "the downgraded allocation should keep decoding video");
    assert!(
        low_max_pixels < high_min_pixels,
        "the low allocation must downgrade decoded spatial resolution (low_max={low_max_pixels}, high_min={high_min_pixels})"
    );
    assert!(
        low_frames.saturating_mul(100) <= high_frames.saturating_mul(75),
        "the low allocation must downgrade temporal cadence (low_frames={low_frames}, high_frames={high_frames})"
    );

    signal_state.set_test_support_available_outgoing_bitrate_bps(
        &room_name,
        SUBSCRIBER_IDENTITY,
        Some(2_000_000),
    );
    tokio::time::sleep(Duration::from_secs(2)).await;
    drain_video_frames_for_duration(&mut video_stream, Duration::from_secs(1)).await;
    let recovered_pixels =
        collect_video_frame_pixels(&mut video_stream, 12, Duration::from_secs(3)).await;
    let recovered_frames = count_video_frames_for_duration(
        &mut video_stream,
        Duration::from_secs(3),
        Duration::from_millis(400),
    )
    .await;
    let recovered_min_pixels = recovered_pixels.iter().copied().min().unwrap_or_default();
    assert!(
        recovered_min_pixels > low_max_pixels,
        "the high allocation must recover decoded spatial resolution (recovered_min={recovered_min_pixels}, low_max={low_max_pixels})"
    );
    assert!(
        recovered_frames > low_frames,
        "the high allocation must recover temporal cadence (recovered_frames={recovered_frames}, low_frames={low_frames})"
    );

    let _ = stop_tx.send(());
    let _ = frame_pump.await;
    let _ = publisher_room.close().await;
    let _ = subscriber_room.close().await;
    server.abort();
}

#[tokio::test]
async fn rust_sdk_room_simulcast_video_quality_switch_preserves_video_delivery_contract() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have local addr");
    let server = tokio::spawn(async move {
        axum::serve(listener, oxidesfu_server::app())
            .await
            .expect("test server should run");
    });

    let room_name = format!("sdk-video-quality-switch-{}", unique_suffix());
    let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-quality-switch-publisher")
        .with_name("SDK Video Quality Switch Publisher")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-video-quality-switch-subscriber")
        .with_name("SDK Video Quality Switch Subscriber")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name,
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);

    let (publisher_room, mut publisher_events) =
        Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
            .await
            .expect("publisher room should connect");
    let (subscriber_room, mut subscriber_events) =
        Room::connect(&format!("http://{addr}"), &subscriber_token, options)
            .await
            .expect("subscriber room should connect");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut subscriber_events).await;

    let source = NativeVideoSource::new(
        VideoResolution {
            width: 1280,
            height: 720,
        },
        false,
    );
    let track = LocalVideoTrack::create_video_track("cam", RtcVideoSource::Native(source.clone()));
    let _publication = publisher_room
        .local_participant()
        .publish_track(
            LocalTrack::Video(track),
            TrackPublishOptions {
                simulcast: true,
                ..Default::default()
            },
        )
        .await
        .expect("publisher should publish simulcast video track");

    let (remote_video_track, remote_publication) =
        wait_for_remote_video_subscription(&mut subscriber_events).await;
    assert!(
        remote_publication.simulcasted(),
        "remote publication should be simulcasted for quality switch contract"
    );

    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let frame_pump_source = source.clone();
    let frame_pump = tokio::spawn(async move {
        let mut ticker = tokio::time::interval(Duration::from_millis(33));
        let mut luma: u8 = 96;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    let mut buffer = livekit::webrtc::prelude::I420Buffer::new(1280, 720);
                    let (y, u, v) = buffer.data_mut();
                    y.fill(luma);
                    u.fill(128);
                    v.fill(128);
                    luma = luma.wrapping_add(1);
                    let frame = livekit::webrtc::prelude::VideoFrame::new(
                        livekit::webrtc::prelude::VideoRotation::VideoRotation0,
                        buffer,
                    );
                    frame_pump_source.capture_frame(&frame);
                }
            }
        }
    });

    let mut video_stream =
        livekit::webrtc::video_stream::native::NativeVideoStream::new(remote_video_track.rtc_track());

    remote_publication.set_video_quality(livekit::track::VideoQuality::High);
    let high_pixels = collect_video_frame_pixels(&mut video_stream, 8, Duration::from_secs(3)).await;
    let baseline_high_pixels = high_pixels.iter().copied().max().unwrap_or(0);
    assert!(
        baseline_high_pixels > 0,
        "high quality frame window should contain non-zero dimensions"
    );

    remote_publication.set_video_quality(livekit::track::VideoQuality::Low);
    let low_pixels = collect_video_frame_pixels(&mut video_stream, 20, Duration::from_secs(3)).await;
    let observed_low_pixels = low_pixels.iter().copied().min().unwrap_or(0);

    assert!(
        observed_low_pixels > 0,
        "low-quality switch should keep delivering decodable video frames"
    );
    assert!(
        observed_low_pixels < baseline_high_pixels,
        "low-quality subscriber frames must be materially smaller than high-quality frames (low_pixels={observed_low_pixels}, high_pixels={baseline_high_pixels})"
    );

    remote_publication.set_video_quality(livekit::track::VideoQuality::High);
    let high_recovery_pixels =
        collect_video_frame_pixels(&mut video_stream, 20, Duration::from_secs(3)).await;
    let recovered_high_pixels = high_recovery_pixels.iter().copied().max().unwrap_or(0);

    assert!(
        recovered_high_pixels > 0,
        "high-quality switch should keep delivering decodable video frames"
    );
    assert!(
        recovered_high_pixels > observed_low_pixels,
        "a low-to-high transition must recover materially larger decoded frames (recovered_pixels={recovered_high_pixels}, low_pixels={observed_low_pixels})"
    );

    // Mirror adaptive-stream layout churn: only the final requested layer may
    // become effective, and it must continue delivering decodable frames.
    remote_publication.set_video_quality(livekit::track::VideoQuality::Low);
    remote_publication.set_video_quality(livekit::track::VideoQuality::High);
    remote_publication.set_video_quality(livekit::track::VideoQuality::Low);
    tokio::time::sleep(Duration::from_millis(250)).await;
    let final_low_pixels =
        collect_video_frame_pixels(&mut video_stream, 20, Duration::from_secs(3)).await;
    assert!(
        final_low_pixels.iter().all(|pixels| *pixels > 0),
        "rapid quality churn must converge to a final low-quality stream that keeps decoding"
    );

    let _ = stop_tx.send(());
    let _ = frame_pump.await;

    let _ = publisher_room.close().await;
    let _ = subscriber_room.close().await;
    server.abort();
}

#[test]
fn rust_sdk_vp9_svc_options_construct_one_l3t3_key_encoding_and_three_spatial_layers() {
    let options = TrackPublishOptions {
        simulcast: false,
        video_codec: livekit::options::VideoCodec::VP9,
        scalability_mode: Some("L3T3_KEY".to_string()),
        ..Default::default()
    };

    let encodings = livekit::options::compute_video_encodings(1280, 720, &options);
    assert_eq!(encodings.len(), 1, "SVC must use one RTP encoding, not simulcast RIDs");
    assert_eq!(encodings[0].scalability_mode.as_deref(), Some("L3T3_KEY"));

    let layers = livekit::options::video_layers_from_encodings(1280, 720, &encodings);
    let dimensions = layers
        .iter()
        .map(|layer| (layer.width, layer.height))
        .collect::<Vec<_>>();
    assert_eq!(dimensions, vec![(320, 180), (640, 360), (1280, 720)]);
}

#[test]
fn rust_sdk_svc_dependency_descriptor_supports_a_three_spatial_layer_switch_frame() {
    use livekit::webrtc::video_frame::{
        DecodeTargetIndication, DependencyDescriptor, DependencyDescriptorStructure,
        DependencyDescriptorTemplate,
    };

    let descriptor = DependencyDescriptor {
        spatial_id: 2,
        temporal_id: 0,
        decode_target_indications: vec![
            DecodeTargetIndication::NotPresent,
            DecodeTargetIndication::NotPresent,
            DecodeTargetIndication::Switch,
        ],
        frame_diffs: Vec::new(),
        chain_diffs: Vec::new(),
        active_decode_targets: 0b111,
        structure: Some(DependencyDescriptorStructure {
            structure_id: 0,
            num_decode_targets: 3,
            num_chains: 0,
            decode_target_protected_by_chain: Vec::new(),
            templates: vec![DependencyDescriptorTemplate {
                spatial_id: 2,
                temporal_id: 0,
                decode_target_indications: vec![
                    DecodeTargetIndication::NotPresent,
                    DecodeTargetIndication::NotPresent,
                    DecodeTargetIndication::Switch,
                ],
                frame_diffs: Vec::new(),
                chain_diffs: Vec::new(),
            }],
        }),
    };

    descriptor
        .validate()
        .expect("pinned SDK should accept a three-spatial-layer switch descriptor");
}

const VP9_SVC_LOW_KEYFRAME: &[u8] =
    include_bytes!("../../fixtures/vp9-svc/low-keyframe.vp9");
const VP9_SVC_HIGH_KEYFRAME: &[u8] =
    include_bytes!("../../fixtures/vp9-svc/high-keyframe.vp9");

fn vp9_svc_switch_descriptor(spatial_id: u8) -> livekit::webrtc::video_frame::DependencyDescriptor {
    use livekit::webrtc::video_frame::{
        DecodeTargetIndication, DependencyDescriptor, DependencyDescriptorStructure,
        DependencyDescriptorTemplate,
    };

    let low = vec![
        DecodeTargetIndication::Switch,
        DecodeTargetIndication::NotPresent,
        DecodeTargetIndication::NotPresent,
    ];
    let medium = vec![
        DecodeTargetIndication::Required,
        DecodeTargetIndication::Switch,
        DecodeTargetIndication::NotPresent,
    ];
    let high = vec![
        DecodeTargetIndication::Required,
        DecodeTargetIndication::Required,
        DecodeTargetIndication::Switch,
    ];
    let decode_target_indications = match spatial_id {
        0 => low.clone(),
        2 => high.clone(),
        _ => panic!("fixture only provides low and high spatial keyframes"),
    };

    DependencyDescriptor {
        spatial_id,
        temporal_id: 0,
        decode_target_indications,
        frame_diffs: Vec::new(),
        chain_diffs: Vec::new(),
        active_decode_targets: if spatial_id == 0 { 0b001 } else { 0b111 },
        structure: Some(DependencyDescriptorStructure {
            structure_id: 0,
            num_decode_targets: 3,
            num_chains: 0,
            decode_target_protected_by_chain: Vec::new(),
            templates: vec![
                DependencyDescriptorTemplate {
                    spatial_id: 0,
                    temporal_id: 0,
                    decode_target_indications: low,
                    frame_diffs: Vec::new(),
                    chain_diffs: Vec::new(),
                },
                DependencyDescriptorTemplate {
                    spatial_id: 1,
                    temporal_id: 0,
                    decode_target_indications: medium,
                    frame_diffs: Vec::new(),
                    chain_diffs: Vec::new(),
                },
                DependencyDescriptorTemplate {
                    spatial_id: 2,
                    temporal_id: 0,
                    decode_target_indications: high,
                    frame_diffs: Vec::new(),
                    chain_diffs: Vec::new(),
                },
            ],
        }),
    }
}

// The fixture validates and is accepted by NativeVideoSource, but the current
// SDK/libwebrtc pre-encoded descriptor path does not deliver a decoded frame to
// the remote NativeVideoStream. Keep the complete contract ready to promote
// once that upstream bridge delivery defect is resolved.
#[tokio::test]
#[ignore = "descriptor-aware pre-encoded VP9 frames are accepted but not delivered by the pinned SDK bridge"]
async fn rust_sdk_room_vp9_svc_quality_low_to_high_preserves_delivery_contract() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let addr = listener
        .local_addr()
        .expect("listener should have a local address");
    let server = tokio::spawn(async move {
        axum::serve(listener, oxidesfu_server::app())
            .await
            .expect("test server should run");
    });

    let room_name = format!("sdk-vp9-svc-quality-switch-{}", unique_suffix());
    let publisher_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-vp9-svc-publisher")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name.clone(),
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("publisher token should encode");
    let subscriber_token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity("sdk-vp9-svc-subscriber")
        .with_grants(VideoGrants {
            room_join: true,
            room: room_name,
            can_publish: true,
            can_subscribe: true,
            ..Default::default()
        })
        .to_jwt()
        .expect("subscriber token should encode");

    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);
    let (publisher_room, mut publisher_events) =
        Room::connect(&format!("http://{addr}"), &publisher_token, options.clone())
            .await
            .expect("publisher room should connect");
    let (subscriber_room, mut subscriber_events) =
        Room::connect(&format!("http://{addr}"), &subscriber_token, options)
            .await
            .expect("subscriber room should connect");
    wait_for_room_connected(&mut publisher_events).await;
    wait_for_room_connected(&mut subscriber_events).await;

    let source = NativeVideoSource::new_encoded(VideoResolution {
        width: 1280,
        height: 720,
    });
    let track = LocalVideoTrack::create_video_track("svc-cam", RtcVideoSource::Native(source.clone()));
    let _publication = publisher_room
        .local_participant()
        .publish_track(
            LocalTrack::Video(track),
            TrackPublishOptions {
                simulcast: false,
                video_codec: livekit::options::VideoCodec::VP9,
                video_encoder: livekit::options::VideoEncoderBackend::PreEncoded,
                scalability_mode: Some("L3T3_KEY".to_string()),
                ..Default::default()
            },
        )
        .await
        .expect("native SDK should publish VP9 L3T3_KEY SVC video");
    let (remote_video_track, remote_publication) =
        wait_for_remote_video_subscription(&mut subscriber_events).await;
    assert!(
        remote_publication.simulcasted(),
        "the SDK SVC publication should advertise switchable spatial layers"
    );

    let (stage_tx, stage_rx) = tokio::sync::watch::channel(0_u8);
    let (stop_tx, mut stop_rx) = tokio::sync::oneshot::channel::<()>();
    let frame_pump_source = source.clone();
    let frame_pump = tokio::spawn(async move {
        use livekit::webrtc::video_frame::{
            EncodedFrameType, EncodedVideoCodec, EncodedVideoFrame, SvcEncodedVideoFrame,
        };

        let mut ticker = tokio::time::interval(Duration::from_millis(100));
        let mut timestamp_us = 1_000_000_i64;
        loop {
            tokio::select! {
                _ = &mut stop_rx => break,
                _ = ticker.tick() => {
                    let (payload, resolution, spatial_id) = if *stage_rx.borrow() == 0 {
                        (
                            VP9_SVC_LOW_KEYFRAME,
                            VideoResolution { width: 320, height: 180 },
                            0,
                        )
                    } else {
                        (
                            VP9_SVC_HIGH_KEYFRAME,
                            VideoResolution { width: 1280, height: 720 },
                            2,
                        )
                    };
                    let frame = SvcEncodedVideoFrame {
                        frame: EncodedVideoFrame {
                            codec: EncodedVideoCodec::VP9,
                            payload,
                            timestamp_us,
                            frame_type: EncodedFrameType::Key,
                            resolution,
                            frame_metadata: None,
                        },
                        dependency_descriptor: vp9_svc_switch_descriptor(spatial_id),
                    };
                    assert!(
                        frame_pump_source
                            .capture_svc_encoded_frame(&frame)
                            .expect("fixture dependency descriptor should validate"),
                        "native source should accept a valid VP9 SVC fixture frame"
                    );
                    timestamp_us += 100_000;
                }
            }
        }
    });
    let mut video_stream =
        livekit::webrtc::video_stream::native::NativeVideoStream::new(remote_video_track.rtc_track());

    remote_publication.set_video_quality(livekit::track::VideoQuality::Low);
    tokio::time::sleep(Duration::from_secs(2)).await;
    drain_video_frames_for_duration(&mut video_stream, Duration::from_secs(1)).await;
    let low_pixels = collect_video_frame_pixels(&mut video_stream, 12, Duration::from_secs(3)).await;
    let low_max_pixels = low_pixels.iter().copied().max().unwrap_or_default();
    assert!(
        low_max_pixels > 0,
        "the requested SVC low layer must decode frames (low_max={low_max_pixels})"
    );
    assert!(
        low_max_pixels <= 640 * 360,
        "the low SVC request must not retain the 1280x720 layer (low_max={low_max_pixels})"
    );

    remote_publication.set_video_quality(livekit::track::VideoQuality::High);
    stage_tx.send(2).expect("fixture pump should remain active");
    tokio::time::sleep(Duration::from_secs(2)).await;
    drain_video_frames_for_duration(&mut video_stream, Duration::from_secs(1)).await;
    let high_pixels = collect_video_frame_pixels(&mut video_stream, 12, Duration::from_secs(3)).await;
    let high_min_pixels = high_pixels.iter().copied().min().unwrap_or_default();
    assert!(
        high_min_pixels > low_max_pixels,
        "the SVC low-to-high request must recover decoded dimensions (low_max={low_max_pixels}, high_min={high_min_pixels})"
    );
    assert!(
        high_pixels.iter().all(|pixels| *pixels > 0),
        "the SVC high-layer transition must keep delivering decodable frames"
    );

    let _ = stop_tx.send(());
    let _ = frame_pump.await;
    let _ = publisher_room.close().await;
    let _ = subscriber_room.close().await;
    server.abort();
}



#[tokio::test]
async fn differential_media_publish_subscribe_event_flow_matches_go_livekit_dev() {
    let _media_probe_guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let ferrite_listener = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("listener should bind");
    let oxidesfu_addr = ferrite_listener
        .local_addr()
        .expect("listener should have local addr");
    let oxidesfu_server = tokio::spawn(async move {
        axum::serve(ferrite_listener, oxidesfu_server::app())
            .await
            .expect("test server should run");
    });

    let Some((mut go_livekit, go_base_url)) = spawn_ready_go_livekit_server_with_single_respawn()
        .await
        .expect("go livekit server should become ready in dev mode")
    else {
        eprintln!("skipping differential test because go is not on PATH");
        oxidesfu_server.abort();
        return;
    };

    let room_name = format!("diff-media-room-{}", unique_suffix());

    let oxidesfu_result = run_media_publish_subscribe_event_flow(
        &format!("http://{oxidesfu_addr}"),
        &room_name,
        "diff-media-alice",
        "diff-media-bob",
    )
    .await;
    let go_result = run_media_publish_subscribe_event_flow(
        &go_base_url,
        &room_name,
        "diff-media-alice",
        "diff-media-bob",
    )
    .await;

    assert_eq!(oxidesfu_result, go_result);

    let _ = go_livekit.kill().await;
    oxidesfu_server.abort();
}

#[derive(Clone, Copy)]
struct ClientInboundMediaSnapshot {
    bytes: u64,
    packets: u64,
    width: u32,
    height: u32,
    decoded: u32,
    dropped: u32,
    discarded: u64,
    pli: u32,
    nack: u32,
}

fn client_media_evidence_delta(
    before: ClientInboundMediaSnapshot,
    after: ClientInboundMediaSnapshot,
    elapsed: Duration,
) -> serde_json::Value {
    let bytes_per_second = (elapsed.as_secs_f64() > 0.0).then(|| {
        ((after.bytes.saturating_sub(before.bytes) as f64 / elapsed.as_secs_f64()) * 1_000.0)
            .round() / 1_000.0
    });
    serde_json::json!({
        "received_bytes": after.bytes,
        "received_packets": after.packets,
        "received_bytes_per_second": bytes_per_second,
        "decoded_dimensions": { "width": after.width, "height": after.height },
        "decoded_frames": after.decoded,
        "dropped_frames": after.dropped,
        "discarded_packets": after.discarded,
        "pli_count": after.pli,
        "nack_count": after.nack,
        "transitions": {
            "decoded_dimensions_changed": before.width != after.width || before.height != after.height,
            "decoded_frames": after.decoded.saturating_sub(before.decoded),
            "pli_count": after.pli.saturating_sub(before.pli),
            "nack_count": after.nack.saturating_sub(before.nack),
        },
        "backpressure": {
            "available": false,
            "value": serde_json::Value::Null,
            "reason": "Rust SDK inbound RTP statistics do not expose server forwarding backpressure"
        }
    })
}

#[test]
fn client_media_evidence_delta_reports_client_stats_without_server_inference() {
    let before = ClientInboundMediaSnapshot { bytes: 1_000, packets: 10, width: 320, height: 180, decoded: 5, dropped: 1, discarded: 2, pli: 3, nack: 4 };
    let after = ClientInboundMediaSnapshot { bytes: 3_500, packets: 30, width: 640, height: 360, decoded: 15, dropped: 2, discarded: 5, pli: 4, nack: 6 };
    let evidence = client_media_evidence_delta(before, after, Duration::from_secs(2));
    assert_eq!(evidence["received_bytes_per_second"], 1_250.0);
    assert_eq!(evidence["decoded_dimensions"]["width"], 640);
    assert_eq!(evidence["transitions"]["decoded_dimensions_changed"], true);
    assert_eq!(evidence["transitions"]["pli_count"], 1);
    assert_eq!(evidence["backpressure"]["available"], false);
    assert!(evidence["backpressure"]["value"].is_null());
}

/// Writes paired-profile post-warm-up media evidence from a Rust SDK client.
#[tokio::test]
#[ignore]
async fn paired_profile_client_media_evidence_writes_post_warmup_track_stats() {
    let _guard = NATIVE_MEDIA_PROBE_LOCK.lock().await;
    let base_url = std::env::var("OXIDESFU_MEDIA_EVIDENCE_BASE_URL").expect("profile server URL is required");
    let room_name = std::env::var("OXIDESFU_MEDIA_EVIDENCE_ROOM").expect("profile room is required");
    let output_path = std::env::var("OXIDESFU_MEDIA_EVIDENCE_OUTPUT").expect("evidence output path is required");
    let duration_from_env = |name, default| std::env::var(name).ok().and_then(|v| v.parse::<u64>().ok()).map(Duration::from_millis).unwrap_or(default);
    let warmup = duration_from_env("OXIDESFU_MEDIA_EVIDENCE_WARMUP_MS", Duration::from_secs(5));
    let window = duration_from_env("OXIDESFU_MEDIA_EVIDENCE_WINDOW_MS", Duration::from_secs(5));
    let interval = duration_from_env("OXIDESFU_MEDIA_EVIDENCE_SAMPLE_INTERVAL_MS", Duration::from_secs(1)).max(Duration::from_millis(100));
    let subscriber_identity = format!("profile-observer-{}", unique_suffix());
    let token = AccessToken::with_api_key(API_KEY, API_SECRET)
        .with_identity(&subscriber_identity)
        .with_grants(VideoGrants { room_join: true, room: room_name.clone(), can_subscribe: true, ..Default::default() })
        .to_jwt().expect("profile observer token should encode");
    let mut options = RoomOptions::default();
    options.single_peer_connection = false;
    options.connect_timeout = Duration::from_secs(10);
    let (room, mut events) = Room::connect(&base_url, &token, options).await.expect("profile observer should connect");
    wait_for_room_connected(&mut events).await;
    let subscriber_sid = room.local_participant().sid().to_string();
    let started = tokio::time::Instant::now();
    let deadline = started + warmup + window;
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    let mut subscriptions = HashMap::new();
    let mut samples = HashMap::new();
    while tokio::time::Instant::now() < deadline {
        tokio::select! {
            event = events.recv() => if let Some(RoomEvent::TrackSubscribed { track, publication, participant }) = event {
                subscriptions.insert(publication.sid().to_string(), (track, publication, participant));
            },
            _ = ticker.tick() => {
                if tokio::time::Instant::now() < started + warmup { continue; }
                for (sid, (track, publication, participant)) in &subscriptions {
                    let kind = match track { livekit::track::RemoteTrack::Audio(_) => "audio", livekit::track::RemoteTrack::Video(_) => "video" };
                    let Ok(stats) = track.get_stats().await else { continue; };
                    let snapshot = stats.iter().find_map(|stat| match stat {
                        livekit::webrtc::stats::RtcStats::InboundRtp(inbound) if inbound.stream.kind == kind => Some(ClientInboundMediaSnapshot {
                            bytes: inbound.inbound.bytes_received, packets: inbound.received.packets_received,
                            width: inbound.inbound.frame_width, height: inbound.inbound.frame_height,
                            decoded: inbound.inbound.frames_decoded, dropped: inbound.inbound.frames_dropped,
                            discarded: inbound.inbound.packets_discarded, pli: inbound.inbound.pli_count, nack: inbound.inbound.nack_count,
                        }),
                        _ => None,
                    });
                    if let Some(snapshot) = snapshot { samples.entry(sid.clone()).and_modify(|entry: &mut (ClientInboundMediaSnapshot, ClientInboundMediaSnapshot, tokio::time::Instant, u32)| {
                        if entry.1.width != snapshot.width || entry.1.height != snapshot.height { entry.3 = entry.3.saturating_add(1); }
                        entry.1 = snapshot;
                    }).or_insert((snapshot, snapshot, tokio::time::Instant::now(), 0)); }
                    let _ = (publication, participant);
                }
            }
        }
    }
    let tracks = subscriptions.into_iter().map(|(sid, (track, publication, participant))| {
        let kind = match &track { livekit::track::RemoteTrack::Audio(_) => "audio", livekit::track::RemoteTrack::Video(_) => "video" };
        let mut report = serde_json::json!({
            "subscriber_identity": subscriber_identity, "subscriber_sid": subscriber_sid,
            "publisher_identity": participant.identity().to_string(), "publisher_sid": participant.sid().to_string(),
            "publication_sid": publication.sid().to_string(), "track_sid": track.sid().to_string(), "kind": kind,
        });
        if let Some((first, latest, first_at, dimension_transitions)) = samples.remove(&sid) {
            let evidence = client_media_evidence_delta(first, latest, tokio::time::Instant::now().saturating_duration_since(first_at));
            report.as_object_mut().expect("evidence report is an object").extend(evidence.as_object().expect("delta is an object").clone());
            report["stats_available"] = serde_json::json!(true);
            report["transitions"]["decoded_dimension_transition_count"] = serde_json::json!(dimension_transitions);
        } else {
            report["stats_available"] = serde_json::json!(false);
            report["unavailable_reason"] = serde_json::json!("no inbound RTP stats sample was available after warm-up");
        }
        report
    }).collect::<Vec<_>>();
    let document = serde_json::json!({
        "schema_version": 1, "source": "rust_sdk_client_webrtc_inbound_rtp_stats", "room": room_name,
        "observer": { "subscriber_identity": subscriber_identity, "subscriber_sid": subscriber_sid },
        "post_warmup": { "warmup_ms": warmup.as_millis(), "observation_window_ms": window.as_millis(), "sample_interval_ms": interval.as_millis() },
        "tracks": tracks,
        "unavailable_fields": { "server_forwarding_backpressure": "not exposed by client-side Rust SDK inbound RTP statistics" }
    });
    std::fs::write(output_path, serde_json::to_vec_pretty(&document).expect("evidence should serialize")).expect("evidence artifact should write");
    room.close().await.expect("profile observer should close");
}
