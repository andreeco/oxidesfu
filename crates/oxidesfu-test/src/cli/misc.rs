use super::*;

#[tokio::test]
async fn livekit_cli_perf_load_test_help_contains_expected_flags() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk perf help test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

    let output = run_lk(["perf", "load-test", "--help"], None)
        .await
        .expect("lk perf load-test --help should run");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert_success(output, "lk perf load-test --help should succeed");

    assert!(
        stdout.contains("--video-publishers"),
        "perf load-test help should expose --video-publishers flag\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains("--audio-publishers"),
        "perf load-test help should expose --audio-publishers flag\nstdout:\n{stdout}"
    );
}

#[tokio::test]
async fn livekit_cli_perf_agent_load_test_help_contains_expected_flags() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk perf agent help test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

    let output = run_lk(["perf", "agent-load-test", "--help"], None)
        .await
        .expect("lk perf agent-load-test --help should run");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert_success(output, "lk perf agent-load-test --help should succeed");

    assert!(
        stdout.contains("--agent-name"),
        "perf agent-load-test help should expose --agent-name flag\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains("--rooms"),
        "perf agent-load-test help should expose --rooms flag\nstdout:\n{stdout}"
    );
}

#[tokio::test]
async fn livekit_cli_deprecated_load_test_alias_help_contains_expected_flags() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk deprecated load-test help test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

    let output = run_lk(["load-test", "--help"], None)
        .await
        .expect("lk load-test --help should run");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert_success(output, "lk deprecated load-test --help should succeed");

    assert!(
        stdout.contains("--video-publishers"),
        "deprecated load-test help should expose --video-publishers flag\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains("--subscribers"),
        "deprecated load-test help should expose --subscribers flag\nstdout:\n{stdout}"
    );
}

#[tokio::test]
async fn livekit_cli_deprecated_join_room_alias_help_contains_expected_flags() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk deprecated join-room help test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

    let output = run_lk(["join-room", "--help"], None)
        .await
        .expect("lk join-room --help should run");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert_success(output, "lk deprecated join-room --help should succeed");

    assert!(
        stdout.contains("--room"),
        "deprecated join-room help should expose --room flag\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains("--identity"),
        "deprecated join-room help should expose --identity flag\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains("--publish-demo"),
        "deprecated join-room help should expose --publish-demo flag\nstdout:\n{stdout}"
    );
}

#[tokio::test]
async fn livekit_cli_perf_load_test_minimal_runtime_smoke() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk perf runtime smoke test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

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

    let room = format!("cli-perf-load-runtime-{}", unique_suffix());
    let url = format!("http://{addr}");
    let output = run_lk(
        [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
            "perf",
            "load-test",
            "--room",
            room.as_str(),
            "--duration",
            "2s",
            "--video-publishers",
            "1",
            "--audio-publishers",
            "0",
            "--subscribers",
            "0",
            "--num-per-second",
            "10",
        ],
        None,
    )
    .await
    .expect("lk perf load-test runtime smoke should run");
    assert_success(
        output,
        "lk perf load-test minimal runtime smoke should succeed against OxideSFU",
    );

    server.abort();
}

#[tokio::test]
async fn livekit_cli_deprecated_load_test_alias_minimal_runtime_smoke() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk deprecated load-test runtime smoke test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

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

    let room = format!("cli-deprecated-load-runtime-{}", unique_suffix());
    let url = format!("http://{addr}");
    let output = run_lk(
        [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
            "load-test",
            "--room",
            room.as_str(),
            "--duration",
            "2s",
            "--video-publishers",
            "1",
            "--audio-publishers",
            "0",
            "--subscribers",
            "0",
            "--num-per-second",
            "10",
        ],
        None,
    )
    .await
    .expect("lk deprecated load-test runtime smoke should run");
    assert_success(
        output,
        "lk deprecated load-test minimal runtime smoke should succeed against OxideSFU",
    );

    server.abort();
}

#[tokio::test]
async fn livekit_cli_perf_load_test_video_publishers_alias_and_layout_runtime_smoke() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk perf alias/layout runtime smoke test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

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

    let room = format!("cli-perf-alias-layout-runtime-{}", unique_suffix());
    let url = format!("http://{addr}");
    let output = run_lk(
        [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
            "perf",
            "load-test",
            "--room",
            room.as_str(),
            "--duration",
            "1s",
            "--publishers",
            "1",
            "--audio-publishers",
            "0",
            "--subscribers",
            "0",
            "--num-per-second",
            "10",
            "--layout",
            "3x3",
        ],
        None,
    )
    .await
    .expect("lk perf load-test alias/layout runtime smoke should run");
    assert_success(
        output,
        "lk perf load-test should accept --publishers alias and non-default --layout against OxideSFU",
    );

    server.abort();
}

#[tokio::test]
async fn livekit_cli_perf_load_test_medium_runtime_mixed_topology_smoke() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk perf medium runtime smoke test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

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

    let room = format!("cli-perf-medium-runtime-mixed-{}", unique_suffix());
    let url = format!("http://{addr}");
    let output = run_lk_with_timeout(
        [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
            "perf",
            "load-test",
            "--room",
            room.as_str(),
            "--duration",
            "8s",
            "--video-publishers",
            "2",
            "--audio-publishers",
            "2",
            "--subscribers",
            "6",
            "--num-per-second",
            "12",
            "--layout",
            "3x3",
            "--video-resolution",
            "medium",
            "--video-codec",
            "vp8",
            "--simulate-speakers",
            "--no-simulcast",
        ],
        None,
        Duration::from_secs(30),
    )
    .await
    .expect("lk perf medium runtime mixed topology smoke should run");
    assert_success(
        output,
        "lk perf medium runtime mixed topology smoke should succeed against OxideSFU",
    );

    server.abort();
}

#[tokio::test]
async fn livekit_cli_deprecated_load_test_medium_runtime_audio_fanout_smoke() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk deprecated medium runtime smoke test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

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

    let room = format!("cli-deprecated-load-medium-audio-{}", unique_suffix());
    let url = format!("http://{addr}");
    let output = run_lk_with_timeout(
        [
            "--url",
            url.as_str(),
            "--api-key",
            API_KEY,
            "--api-secret",
            API_SECRET,
            "--yes",
            "load-test",
            "--room",
            room.as_str(),
            "--duration",
            "8s",
            "--video-publishers",
            "0",
            "--audio-publishers",
            "3",
            "--subscribers",
            "8",
            "--num-per-second",
            "12",
            "--layout",
            "speaker",
            "--simulate-speakers",
            "--no-simulcast",
        ],
        None,
        Duration::from_secs(30),
    )
    .await
    .expect("lk deprecated medium runtime audio fanout smoke should run");
    assert_success(
        output,
        "lk deprecated medium runtime audio fanout smoke should succeed against OxideSFU",
    );

    server.abort();
}

#[tokio::test]
async fn livekit_cli_agent_simulate_help_contains_expected_flags() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping lk agent simulate help test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

    let output = run_lk(["agent", "simulate", "--help"], None)
        .await
        .expect("lk agent simulate --help should run");
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    assert_success(output, "lk agent simulate --help should succeed");

    assert!(
        stdout.contains("--scenarios"),
        "agent simulate help should expose --scenarios flag\nstdout:\n{stdout}"
    );
    assert!(
        stdout.contains("--num-simulations"),
        "agent simulate help should expose --num-simulations flag\nstdout:\n{stdout}"
    );
}
