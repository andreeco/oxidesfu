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
