use super::*;

// The load tester's video reader uses an H.264 depacketizer. Pinning the publisher codec keeps
// received-packet accounting comparable between the Go baseline and OxideSFU.
const BENCHMARK_VIDEO_CODEC: &str = "h264";

#[derive(Debug, Clone, Copy)]
struct BenchmarkScenario {
    name: &'static str,
    duration: &'static str,
    video_publishers: u16,
    audio_publishers: u16,
    subscribers: u16,
    num_per_second: &'static str,
    layout: &'static str,
    video_resolution: &'static str,
    no_simulcast: bool,
}

#[derive(Debug, Clone)]
struct ResourceSample {
    elapsed_ms: u128,
    cpu_jiffies: u64,
    rss_kib: u64,
    loadavg_1m: f64,
    net_rx_bytes: Option<u64>,
    net_tx_bytes: Option<u64>,
    net_rx_packets: Option<u64>,
    net_tx_packets: Option<u64>,
    fd_count: Option<u64>,
}

#[derive(Debug, Clone)]
struct ResourceSummary {
    sample_count: usize,
    wall_seconds: f64,
    cpu_seconds: f64,
    approx_cpu_percent_one_core: f64,
    peak_rss_kib: u64,
    avg_rss_kib: u64,
    peak_loadavg_1m: f64,
    avg_loadavg_1m: f64,
    net_rx_bytes_delta: Option<u64>,
    net_tx_bytes_delta: Option<u64>,
    net_rx_packets_delta: Option<u64>,
    net_tx_packets_delta: Option<u64>,
    peak_fd_count: Option<u64>,
}

#[derive(Debug)]
struct ServerBenchmarkResult {
    status_success: bool,
    stdout: String,
    stderr: String,
    summary: ResourceSummary,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum BenchmarkMode {
    Smoke,
    Full,
}

#[derive(Debug, Clone, Copy)]
struct BenchmarkConfig {
    mode: BenchmarkMode,
    runs: usize,
    max_wall_regression_percent: f64,
    max_cpu_regression_percent: f64,
    max_peak_rss_regression_percent: f64,
}

#[derive(Debug, Clone, Copy)]
struct MetricStats {
    min: f64,
    median: f64,
    p95: f64,
    max: f64,
    mean: f64,
}

#[derive(Debug, Clone)]
struct AggregatedResourceSummary {
    wall_seconds: MetricStats,
    cpu_seconds: MetricStats,
    approx_cpu_percent_one_core: MetricStats,
    peak_rss_kib: MetricStats,
}

#[derive(Debug)]
struct ImplementationBenchmarkResult {
    implementation: &'static str,
    status_success: bool,
    successful_runs: usize,
    total_runs: usize,
    runs: Vec<ServerBenchmarkResult>,
    aggregated: AggregatedResourceSummary,
}

#[tokio::test]
async fn benchmark_compare_audio_fanout_small_cpu_rss() {
    run_benchmark_comparison_if_enabled(BenchmarkScenario {
        name: "audio_fanout_small",
        duration: "6s",
        video_publishers: 0,
        audio_publishers: 2,
        subscribers: 4,
        num_per_second: "10",
        layout: "speaker",
        video_resolution: "low",
        no_simulcast: true,
    })
    .await;
}

#[tokio::test]
async fn benchmark_compare_video_room_small_cpu_rss() {
    run_benchmark_comparison_if_enabled(BenchmarkScenario {
        name: "video_room_small",
        duration: "6s",
        video_publishers: 1,
        audio_publishers: 0,
        subscribers: 2,
        num_per_second: "10",
        layout: "3x3",
        video_resolution: "low",
        no_simulcast: true,
    })
    .await;
}

#[tokio::test]
async fn benchmark_compare_audio_fanout_medium_cpu_rss() {
    run_benchmark_comparison_if_enabled(BenchmarkScenario {
        name: "audio_fanout_medium",
        duration: "10s",
        video_publishers: 0,
        audio_publishers: 4,
        subscribers: 12,
        num_per_second: "12",
        layout: "speaker",
        video_resolution: "low",
        no_simulcast: true,
    })
    .await;
}

#[tokio::test]
async fn benchmark_compare_livestream_medium_cpu_rss() {
    run_benchmark_comparison_if_enabled(BenchmarkScenario {
        name: "livestream_medium",
        duration: "10s",
        video_publishers: 1,
        audio_publishers: 0,
        subscribers: 20,
        num_per_second: "15",
        layout: "speaker",
        video_resolution: "low",
        no_simulcast: true,
    })
    .await;
}

#[tokio::test]
async fn benchmark_compare_mixed_room_medium_cpu_rss() {
    run_benchmark_comparison_if_enabled(BenchmarkScenario {
        name: "mixed_room_medium",
        duration: "10s",
        video_publishers: 2,
        audio_publishers: 2,
        subscribers: 10,
        num_per_second: "12",
        layout: "3x3",
        video_resolution: "low",
        no_simulcast: true,
    })
    .await;
}

#[tokio::test]
async fn benchmark_compare_video_room_high_simulcast_large_cpu_rss() {
    run_benchmark_comparison_if_enabled(BenchmarkScenario {
        name: "video_room_high_simulcast_large",
        duration: "30s",
        video_publishers: 3,
        audio_publishers: 0,
        subscribers: 18,
        num_per_second: "20",
        layout: "3x3",
        video_resolution: "high",
        no_simulcast: false,
    })
    .await;
}

#[tokio::test]
async fn benchmark_compare_mixed_room_high_simulcast_large_cpu_rss() {
    run_benchmark_comparison_if_enabled(BenchmarkScenario {
        name: "mixed_room_high_simulcast_large",
        duration: "30s",
        video_publishers: 4,
        audio_publishers: 4,
        subscribers: 20,
        num_per_second: "20",
        layout: "speaker",
        video_resolution: "high",
        no_simulcast: false,
    })
    .await;
}

#[tokio::test]
async fn benchmark_resource_summary_computes_cpu_and_memory() {
    let samples = vec![
        ResourceSample {
            elapsed_ms: 0,
            cpu_jiffies: 100,
            rss_kib: 10_000,
            loadavg_1m: 0.5,
            net_rx_bytes: None,
            net_tx_bytes: None,
            net_rx_packets: None,
            net_tx_packets: None,
            fd_count: None,
        },
        ResourceSample {
            elapsed_ms: 1_000,
            cpu_jiffies: 150,
            rss_kib: 20_000,
            loadavg_1m: 1.5,
            net_rx_bytes: None,
            net_tx_bytes: None,
            net_rx_packets: None,
            net_tx_packets: None,
            fd_count: None,
        },
    ];

    let summary = summarize_samples(&samples, 100);
    assert_eq!(summary.sample_count, 2);
    assert_eq!(summary.wall_seconds, 1.0);
    assert_eq!(summary.cpu_seconds, 0.5);
    assert_eq!(summary.approx_cpu_percent_one_core, 50.0);
    assert_eq!(summary.peak_rss_kib, 20_000);
    assert_eq!(summary.avg_rss_kib, 15_000);
    assert_eq!(summary.peak_loadavg_1m, 1.5);
    assert_eq!(summary.avg_loadavg_1m, 1.0);
    assert_eq!(summary.net_rx_bytes_delta, None);
    assert_eq!(summary.net_tx_bytes_delta, None);
    assert_eq!(summary.net_rx_packets_delta, None);
    assert_eq!(summary.net_tx_packets_delta, None);
    assert_eq!(summary.peak_fd_count, None);
}

#[tokio::test]
async fn benchmark_resource_summary_records_network_and_packet_deltas_if_available() {
    let samples = vec![
        ResourceSample {
            elapsed_ms: 0,
            cpu_jiffies: 10,
            rss_kib: 1_000,
            loadavg_1m: 0.1,
            net_rx_bytes: Some(10_000),
            net_tx_bytes: Some(20_000),
            net_rx_packets: Some(100),
            net_tx_packets: Some(200),
            fd_count: Some(16),
        },
        ResourceSample {
            elapsed_ms: 2_000,
            cpu_jiffies: 30,
            rss_kib: 2_000,
            loadavg_1m: 0.4,
            net_rx_bytes: Some(12_500),
            net_tx_bytes: Some(28_000),
            net_rx_packets: Some(125),
            net_tx_packets: Some(280),
            fd_count: Some(32),
        },
    ];

    let summary = summarize_samples(&samples, 100);
    assert_eq!(summary.net_rx_bytes_delta, Some(2_500));
    assert_eq!(summary.net_tx_bytes_delta, Some(8_000));
    assert_eq!(summary.net_rx_packets_delta, Some(25));
    assert_eq!(summary.net_tx_packets_delta, Some(80));
    assert_eq!(summary.peak_fd_count, Some(32));
}

#[tokio::test]
async fn benchmark_resource_summary_handles_short_runs_without_divide_by_zero() {
    let samples = vec![
        ResourceSample {
            elapsed_ms: 5,
            cpu_jiffies: 1,
            rss_kib: 100,
            loadavg_1m: 0.0,
            net_rx_bytes: None,
            net_tx_bytes: None,
            net_rx_packets: None,
            net_tx_packets: None,
            fd_count: None,
        },
        ResourceSample {
            elapsed_ms: 5,
            cpu_jiffies: 2,
            rss_kib: 100,
            loadavg_1m: 0.0,
            net_rx_bytes: None,
            net_tx_bytes: None,
            net_rx_packets: None,
            net_tx_packets: None,
            fd_count: None,
        },
    ];

    let summary = summarize_samples(&samples, 100);
    assert!(summary.wall_seconds >= 0.001);
    assert!(summary.approx_cpu_percent_one_core.is_finite());
}

#[test]
fn benchmark_load_output_requires_all_subscribers_to_receive_tracks_and_packets() {
    let scenario = BenchmarkScenario {
        name: "video_room_small",
        duration: "6s",
        video_publishers: 1,
        audio_publishers: 0,
        subscribers: 2,
        num_per_second: "10",
        layout: "3x3",
        video_resolution: "low",
        no_simulcast: true,
    };
    let output = "Subscriber summaries:\n\
│ Sub 0  │ 0/1    │ NaNmbps │ - │ - │\n\
│ Sub 1  │ 0/1    │ NaNmbps │ - │ - │\n";

    let error = validate_load_test_output(output, scenario).expect_err("missing media must fail");
    assert!(error.contains("Sub 0 reports 0/1 tracks"));
}

#[test]
fn benchmark_load_output_accepts_received_audio_fanout() {
    let scenario = BenchmarkScenario {
        name: "audio_fanout_small",
        duration: "6s",
        video_publishers: 0,
        audio_publishers: 2,
        subscribers: 2,
        num_per_second: "10",
        layout: "speaker",
        video_resolution: "low",
        no_simulcast: true,
    };
    let output = "Track loading:\n\
│ Sub 0  │ track-a │ audio │ 290 │ 19.5kbps │ 0 (0%) │\n\
│        │ track-b │ audio │ 291 │ 19.5kbps │ 0 (0%) │\n\
│ Sub 1  │ track-a │ audio │ 292 │ 19.5kbps │ 0 (0%) │\n\
│        │ track-b │ audio │ 293 │ 19.5kbps │ 0 (0%) │\n\
Subscriber summaries:\n\
│ Sub 0  │ 2/2    │ 41.2kbps │ 0 │ - │\n\
│ Sub 1  │ 2/2    │ 41.2kbps │ 0 │ - │\n";

    validate_load_test_output(output, scenario).expect("received media should pass");
}

#[test]
fn benchmark_load_output_rejects_zero_packets_for_any_expected_track() {
    let scenario = BenchmarkScenario {
        name: "audio_fanout_small",
        duration: "6s",
        video_publishers: 0,
        audio_publishers: 2,
        subscribers: 1,
        num_per_second: "10",
        layout: "speaker",
        video_resolution: "low",
        no_simulcast: true,
    };
    let output = "Track loading:\n\
│ Sub 0  │ track-a │ audio │ 0 │ 0bps │ 0 (0%) │\n\
│        │ track-b │ audio │ 290 │ 19.5kbps │ 0 (0%) │\n\
Subscriber summaries:\n\
│ Sub 0  │ 2/2    │ 19.5kbps │ 0 │ - │\n";

    let error = validate_load_test_output(output, scenario)
        .expect_err("every expected track must receive RTP");
    assert!(error.contains("track-a has no received packets"));
}

#[test]
fn benchmark_load_output_rejects_materially_lower_packet_delivery_than_go() {
    let go = "Track loading:\n\
│ Sub 0  │ track-a │ video │ 100 │ 100kbps │ 0 (0%) │\n";
    let oxidesfu = "Track loading:\n\
│ Sub 0  │ track-a │ video │ 48 │ 48kbps │ 0 (0%) │\n";

    let error = ensure_comparable_packet_delivery(go, oxidesfu)
        .expect_err("Rust delivery below half of Go must fail");
    assert!(error.contains("48; expected totals"), "{error}");
}

#[test]
fn benchmark_load_output_accepts_comparable_packet_delivery() {
    let go = "Track loading:\n\
│ Sub 0  │ track-a │ video │ 100 │ 100kbps │ 0 (0%) │\n";
    let oxidesfu = "Track loading:\n\
│ Sub 0  │ track-a │ video │ 150 │ 150kbps │ 0 (0%) │\n";

    ensure_comparable_packet_delivery(go, oxidesfu)
        .expect("delivery within the two-times envelope should pass");
}

#[test]
fn benchmark_load_output_rejects_subscriber_with_no_packets() {
    let scenario = BenchmarkScenario {
        name: "video_room_small",
        duration: "6s",
        video_publishers: 1,
        audio_publishers: 0,
        subscribers: 1,
        num_per_second: "10",
        layout: "3x3",
        video_resolution: "low",
        no_simulcast: true,
    };
    let output = "Track loading:\n\
│ Sub 0  │ track-a │ video │ 0 │ 0bps │ 0 (0%) │\n\
Subscriber summaries:\n\
│ Sub 0  │ 1/1    │ 0bps │ 0 │ - │\n";

    let error = validate_load_test_output(output, scenario).expect_err("zero packets must fail");
    assert!(error.contains("Sub 0 track track-a has no received packets"));
}

#[tokio::test]
async fn benchmark_environment_reports_file_descriptor_limit() {
    let metadata = environment_metadata();
    assert!(metadata.get("fd_limit_soft").is_some());
    assert!(metadata.get("fd_limit_hard").is_some());
}

#[tokio::test]
async fn benchmark_writes_summary_to_target_benchmarks_and_includes_thresholds_and_scenario_config() {
    let config = BenchmarkConfig {
        mode: BenchmarkMode::Smoke,
        runs: 1,
        max_wall_regression_percent: 10.0,
        max_cpu_regression_percent: 20.0,
        max_peak_rss_regression_percent: 30.0,
    };
    let scenario = BenchmarkScenario {
        name: "unit_summary_output",
        duration: "1s",
        video_publishers: 1,
        audio_publishers: 1,
        subscribers: 2,
        num_per_second: "5",
        layout: "speaker",
        video_resolution: "low",
        no_simulcast: true,
    };

    let summary = ResourceSummary {
        sample_count: 2,
        wall_seconds: 1.0,
        cpu_seconds: 0.5,
        approx_cpu_percent_one_core: 50.0,
        peak_rss_kib: 1024,
        avg_rss_kib: 900,
        peak_loadavg_1m: 1.0,
        avg_loadavg_1m: 0.7,
        net_rx_bytes_delta: Some(123),
        net_tx_bytes_delta: Some(456),
        net_rx_packets_delta: Some(3),
        net_tx_packets_delta: Some(6),
        peak_fd_count: Some(64),
    };
    let run = ServerBenchmarkResult {
        status_success: true,
        stdout: String::new(),
        stderr: String::new(),
        summary: summary.clone(),
    };
    let go = aggregate_implementation_results("go_livekit", vec![run]);
    let oxide = aggregate_implementation_results(
        "oxidesfu",
        vec![ServerBenchmarkResult {
            status_success: true,
            stdout: String::new(),
            stderr: String::new(),
            summary,
        }],
    );

    let dir = write_benchmark_artifacts(config, scenario, &[&go, &oxide])
        .expect("benchmark artifact write should succeed in unit test");

    assert!(dir.ends_with("target/benchmarks"));
    let mut latest_json: Option<PathBuf> = None;
    let mut latest_md: Option<PathBuf> = None;
    for entry in std::fs::read_dir(&dir).expect("artifact dir should be readable") {
        let entry = entry.expect("dir entry should be readable");
        let path = entry.path();
        let Some(name) = path.file_name().and_then(|name| name.to_str()) else {
            continue;
        };
        if name.starts_with("unit_summary_output-") && name.ends_with(".json") {
            let is_newer = latest_json
                .as_ref()
                .and_then(|previous| previous.file_name())
                .and_then(|previous| previous.to_str())
                .is_none_or(|previous| name > previous);
            if is_newer {
                latest_json = Some(path.clone());
            }
        }
        if name.starts_with("unit_summary_output-") && name.ends_with(".md") {
            let is_newer = latest_md
                .as_ref()
                .and_then(|previous| previous.file_name())
                .and_then(|previous| previous.to_str())
                .is_none_or(|previous| name > previous);
            if is_newer {
                latest_md = Some(path);
            }
        }
    }

    let json_path = latest_json.expect("scenario json artifact should exist");
    let md_path = latest_md.expect("scenario markdown artifact should exist");

    let raw = std::fs::read_to_string(json_path).expect("json artifact should be readable");
    let value: serde_json::Value = serde_json::from_str(&raw).expect("json artifact should parse");

    assert_eq!(value["config"]["max_wall_regression_percent"], 10.0);
    assert_eq!(value["config"]["max_cpu_regression_percent"], 20.0);
    assert_eq!(value["config"]["max_peak_rss_regression_percent"], 30.0);
    assert_eq!(value["scenario"]["name"], "unit_summary_output");
    assert_eq!(value["scenario"]["video_publishers"], 1);
    assert_eq!(value["scenario"]["video_codec"], BENCHMARK_VIDEO_CODEC);

    let markdown = std::fs::read_to_string(md_path).expect("markdown artifact should be readable");
    assert!(markdown.contains("Gates: wall `10.0%`, cpu `20.0%`, peak RSS `30.0%`"));
}

#[tokio::test]
async fn benchmark_summary_includes_git_revision_if_available() {
    let metadata = environment_metadata();
    assert!(metadata.get("git_revision").is_some());
}

fn benchmark_mode() -> BenchmarkMode {
    match std::env::var("OXIDESFU_BENCHMARK_MODE") {
        Ok(value) if value.eq_ignore_ascii_case("full") => BenchmarkMode::Full,
        _ => BenchmarkMode::Smoke,
    }
}

fn benchmark_config() -> BenchmarkConfig {
    let mode = benchmark_mode();
    let default_runs = match mode {
        BenchmarkMode::Smoke => 1,
        BenchmarkMode::Full => 5,
    };

    let runs = std::env::var("OXIDESFU_BENCHMARK_RUNS")
        .ok()
        .and_then(|v| v.parse::<usize>().ok())
        .filter(|v| *v > 0)
        .unwrap_or(default_runs);

    let max_wall_regression_percent = std::env::var("OXIDESFU_BENCHMARK_MAX_WALL_REGRESSION_PERCENT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(25.0);
    let max_cpu_regression_percent = std::env::var("OXIDESFU_BENCHMARK_MAX_CPU_REGRESSION_PERCENT")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(25.0);
    let max_peak_rss_regression_percent =
        std::env::var("OXIDESFU_BENCHMARK_MAX_PEAK_RSS_REGRESSION_PERCENT")
            .ok()
            .and_then(|v| v.parse::<f64>().ok())
            .unwrap_or(25.0);

    BenchmarkConfig {
        mode,
        runs,
        max_wall_regression_percent,
        max_cpu_regression_percent,
        max_peak_rss_regression_percent,
    }
}

fn should_run_scenario_for_mode(config: BenchmarkConfig, scenario: BenchmarkScenario) -> bool {
    match config.mode {
        BenchmarkMode::Full => true,
        BenchmarkMode::Smoke => {
            !scenario.name.contains("medium")
                && !scenario.name.contains("livestream")
                && !scenario.name.contains("large")
                && !scenario.name.contains("xlarge")
        }
    }
}

fn metric_stats(values: &[f64]) -> MetricStats {
    let mut sorted = values.to_vec();
    sorted.sort_by(f64::total_cmp);
    let len = sorted.len();
    let median_index = len / 2;
    let p95_index = ((len as f64) * 0.95).ceil() as usize;
    let p95_index = p95_index.saturating_sub(1).min(len.saturating_sub(1));
    let mean = if sorted.is_empty() {
        0.0
    } else {
        sorted.iter().sum::<f64>() / sorted.len() as f64
    };

    MetricStats {
        min: round3(*sorted.first().unwrap_or(&0.0)),
        median: round3(*sorted.get(median_index).unwrap_or(&0.0)),
        p95: round3(*sorted.get(p95_index).unwrap_or(&0.0)),
        max: round3(*sorted.last().unwrap_or(&0.0)),
        mean: round3(mean),
    }
}

fn aggregate_implementation_results(
    implementation: &'static str,
    runs: Vec<ServerBenchmarkResult>,
) -> ImplementationBenchmarkResult {
    let successful_runs = runs.iter().filter(|run| run.status_success).count();
    let total_runs = runs.len();
    let status_success = successful_runs == total_runs;

    let wall = runs
        .iter()
        .map(|run| run.summary.wall_seconds)
        .collect::<Vec<_>>();
    let cpu = runs
        .iter()
        .map(|run| run.summary.cpu_seconds)
        .collect::<Vec<_>>();
    let cpu_percent = runs
        .iter()
        .map(|run| run.summary.approx_cpu_percent_one_core)
        .collect::<Vec<_>>();
    let peak_rss_kib = runs
        .iter()
        .map(|run| run.summary.peak_rss_kib as f64)
        .collect::<Vec<_>>();

    ImplementationBenchmarkResult {
        implementation,
        status_success,
        successful_runs,
        total_runs,
        runs,
        aggregated: AggregatedResourceSummary {
            wall_seconds: metric_stats(&wall),
            cpu_seconds: metric_stats(&cpu),
            approx_cpu_percent_one_core: metric_stats(&cpu_percent),
            peak_rss_kib: metric_stats(&peak_rss_kib),
        },
    }
}

fn assert_regression_gates(
    config: BenchmarkConfig,
    go: &ImplementationBenchmarkResult,
    oxidesfu: &ImplementationBenchmarkResult,
) {
    let wall_limit = go.aggregated.wall_seconds.median
        * (1.0 + config.max_wall_regression_percent / 100.0);
    let cpu_limit = go.aggregated.cpu_seconds.median
        * (1.0 + config.max_cpu_regression_percent / 100.0);
    let rss_limit = go.aggregated.peak_rss_kib.median
        * (1.0 + config.max_peak_rss_regression_percent / 100.0);

    assert!(
        oxidesfu.aggregated.wall_seconds.median <= wall_limit,
        "wall median regression too high: oxidesfu={:.3}s go={:.3}s limit={:.3}s",
        oxidesfu.aggregated.wall_seconds.median,
        go.aggregated.wall_seconds.median,
        wall_limit,
    );
    assert!(
        oxidesfu.aggregated.cpu_seconds.median <= cpu_limit,
        "cpu median regression too high: oxidesfu={:.3}s go={:.3}s limit={:.3}s",
        oxidesfu.aggregated.cpu_seconds.median,
        go.aggregated.cpu_seconds.median,
        cpu_limit,
    );
    assert!(
        oxidesfu.aggregated.peak_rss_kib.median <= rss_limit,
        "peak RSS median regression too high: oxidesfu={:.0}KiB go={:.0}KiB limit={:.0}KiB",
        oxidesfu.aggregated.peak_rss_kib.median,
        go.aggregated.peak_rss_kib.median,
        rss_limit,
    );
}

async fn run_benchmark_comparison_if_enabled(scenario: BenchmarkScenario) {
    if std::env::var_os("OXIDESFU_ENABLE_BENCHMARKS").is_none() {
        eprintln!(
            "skipping benchmark comparison {} unless OXIDESFU_ENABLE_BENCHMARKS=1 is set",
            scenario.name
        );
        return;
    }

    // Scenarios share the host network namespace and the load generator. Serialize them so
    // background traffic from another scenario cannot distort this scenario's measurements.
    let _benchmark_run_guard = benchmark_run_lock().lock().await;

    let config = benchmark_config();
    if let Some((soft_limit, _hard_limit)) = read_self_fd_limits()
        && soft_limit < 65_535
    {
        eprintln!(
            "benchmark warning: soft open-files limit is {} (< 65535); high-scale load scenarios may under-report capacity",
            soft_limit
        );
    }
    if !should_run_scenario_for_mode(config, scenario) {
        eprintln!(
            "skipping benchmark comparison {} in {:?} mode",
            scenario.name, config.mode
        );
        return;
    }

    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping benchmark comparison because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run before benchmarks");

    let Some((mut go_livekit, go_base_url)) = spawn_ready_go_livekit_server_with_single_respawn()
        .await
        .expect("go livekit server should become ready for benchmark comparison")
    else {
        eprintln!("skipping benchmark comparison because Go or LiveKit server is unavailable");
        return;
    };

    let oxidesfu_port = reserve_local_port();
    let Some((mut oxidesfu, oxidesfu_base_url)) = spawn_oxidesfu_benchmark_server(oxidesfu_port)
        .await
        .expect("oxidesfu benchmark server should start")
    else {
        eprintln!("skipping benchmark comparison because oxidesfu-server binary is unavailable");
        let _ = go_livekit.kill().await;
        return;
    };

    let go_pid = go_livekit
        .id()
        .expect("Go LiveKit benchmark child should expose pid");
    let oxidesfu_pid = oxidesfu
        .id()
        .expect("OxideSFU benchmark child should expose pid");

    let mut go_runs = Vec::with_capacity(config.runs);
    let mut oxidesfu_runs = Vec::with_capacity(config.runs);
    for _ in 0..config.runs {
        let go_run = run_lk_load_benchmark("go_livekit", go_pid, &go_base_url, scenario)
            .await
            .expect("Go LiveKit benchmark run should complete");
        let mut oxidesfu_run = run_lk_load_benchmark("oxidesfu", oxidesfu_pid, &oxidesfu_base_url, scenario)
            .await
            .expect("OxideSFU benchmark run should complete");
        if let Err(error) = ensure_comparable_packet_delivery(&go_run.stdout, &oxidesfu_run.stdout) {
            oxidesfu_run.status_success = false;
            if !oxidesfu_run.stderr.is_empty() {
                oxidesfu_run.stderr.push('\n');
            }
            oxidesfu_run
                .stderr
                .push_str("load-test delivery comparison failed: ");
            oxidesfu_run.stderr.push_str(&error);
        }
        go_runs.push(go_run);
        oxidesfu_runs.push(oxidesfu_run);
    }

    let go_result = aggregate_implementation_results("go_livekit", go_runs);
    let oxidesfu_result = aggregate_implementation_results("oxidesfu", oxidesfu_runs);

    let artifact_dir = write_benchmark_artifacts(config, scenario, &[&go_result, &oxidesfu_result])
        .expect("benchmark artifacts should be written");
    eprintln!(
        "benchmark artifacts for {} written to {}",
        scenario.name,
        artifact_dir.display()
    );

    assert!(
        go_result.status_success,
        "Go LiveKit benchmark load-test failed in {}/{} runs",
        go_result.total_runs.saturating_sub(go_result.successful_runs),
        go_result.total_runs,
    );
    assert!(
        oxidesfu_result.status_success,
        "OxideSFU benchmark load-test failed in {}/{} runs",
        oxidesfu_result.total_runs.saturating_sub(oxidesfu_result.successful_runs),
        oxidesfu_result.total_runs,
    );

    assert!(
        go_result
            .runs
            .iter()
            .all(|result| result.summary.sample_count >= 2)
    );
    assert!(
        oxidesfu_result
            .runs
            .iter()
            .all(|result| result.summary.sample_count >= 2)
    );

    assert_regression_gates(config, &go_result, &oxidesfu_result);

    let _ = oxidesfu.kill().await;
    let _ = go_livekit.kill().await;
}

fn oxidesfu_benchmark_server_binary_path() -> PathBuf {
    oxidesfu_workspace_root().join("target/release/oxidesfu-server")
}

async fn ensure_oxidesfu_benchmark_server_binary_built() -> Result<bool, String> {
    let binary_path = oxidesfu_benchmark_server_binary_path();

    let mut build = tokio::process::Command::new("cargo");
    build
        .arg("build")
        .arg("-p")
        .arg("oxidesfu-server")
        .arg("--release")
        .current_dir(oxidesfu_workspace_root())
        .stdout(Stdio::null())
        .stderr(Stdio::null());

    let status = match tokio::time::timeout(Duration::from_secs(900), build.status()).await {
        Ok(Ok(status)) => status,
        Ok(Err(err)) if err.kind() == ErrorKind::NotFound => return Ok(false),
        Ok(Err(err)) => return Err(format!("failed to execute cargo build --release: {err}")),
        Err(_) => {
            return Err(
                "cargo build --release timed out while preparing oxidesfu-server benchmark binary"
                    .to_string(),
            );
        }
    };

    if !status.success() {
        return Err(format!(
            "cargo build -p oxidesfu-server --release failed with status {status}"
        ));
    }

    Ok(binary_path.exists())
}

async fn spawn_oxidesfu_benchmark_server(
    bind_port: u16,
) -> Result<Option<(tokio::process::Child, String)>, String> {
    if !ensure_oxidesfu_benchmark_server_binary_built().await? {
        return Ok(None);
    }

    let bind = format!("127.0.0.1:{bind_port}");
    let rtc_tcp_port = reserve_local_port();
    let mut command = tokio::process::Command::new(oxidesfu_benchmark_server_binary_path());
    command.kill_on_drop(true);
    command
        .arg("--bind")
        .arg(&bind)
        .arg("--rtc-tcp-port")
        .arg(rtc_tcp_port.to_string())
        .arg("--api-key")
        .arg(API_KEY)
        .arg("--api-secret")
        .arg(API_SECRET)
        .env("RUST_LOG", "error")
        .current_dir(oxidesfu_workspace_root());

    if std::env::var_os("OXIDESFU_BENCHMARK_SERVER_STDIO").is_some() {
        command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
    } else {
        command.stdout(Stdio::null()).stderr(Stdio::null());
    }

    let mut child = command
        .spawn()
        .map_err(|err| format!("failed to spawn oxidesfu-server benchmark process: {err}"))?;
    let base_url = format!("http://127.0.0.1:{bind_port}");
    match wait_for_room_service_ready_with_retry_and_process(
        &base_url,
        Duration::from_secs(20),
        Duration::from_millis(100),
        Duration::from_millis(800),
        Some(&mut child),
    )
    .await
    {
        Ok(()) => Ok(Some((child, base_url))),
        Err(err) => {
            let _ = child.kill().await;
            Err(format!(
                "oxidesfu benchmark process did not become ready: {err}"
            ))
        }
    }
}

fn benchmark_run_lock() -> &'static tokio::sync::Mutex<()> {
    static LOCK: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    LOCK.get_or_init(|| tokio::sync::Mutex::new(()))
}

async fn run_lk_load_benchmark(
    implementation: &'static str,
    server_pid: u32,
    base_url: &str,
    scenario: BenchmarkScenario,
) -> Result<ServerBenchmarkResult, String> {
    let ticks_per_second = clock_ticks_per_second();
    let stop = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let sampler_stop = Arc::clone(&stop);
    let sampler = tokio::spawn(async move {
        sample_process_resources_until_stopped(
            server_pid,
            ticks_per_second,
            Duration::from_millis(200),
            sampler_stop,
        )
        .await
    });

    let room = format!("benchmark-{}-{}", scenario.name, unique_suffix());
    let video_publishers = scenario.video_publishers.to_string();
    let audio_publishers = scenario.audio_publishers.to_string();
    let subscribers = scenario.subscribers.to_string();
    let mut args = vec![
        "--url",
        base_url,
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
        scenario.duration,
        "--video-publishers",
        video_publishers.as_str(),
        "--audio-publishers",
        audio_publishers.as_str(),
        "--subscribers",
        subscribers.as_str(),
        "--num-per-second",
        scenario.num_per_second,
        "--layout",
        scenario.layout,
        "--video-resolution",
        scenario.video_resolution,
        "--video-codec",
        BENCHMARK_VIDEO_CODEC,
    ];
    if scenario.no_simulcast {
        args.push("--no-simulcast");
    }

    let mut command = lk_command(args, None);
    command.kill_on_drop(true);
    let timeout = parse_duration_seconds(scenario.duration)
        .map(|seconds| Duration::from_secs(seconds + 90))
        .unwrap_or(Duration::from_secs(120));
    let output = match tokio::time::timeout(timeout, command.output()).await {
        Ok(Ok(output)) => output,
        Ok(Err(err)) if err.kind() == ErrorKind::NotFound => {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = sampler.await;
            return Err("lk is not on PATH".to_string());
        }
        Ok(Err(err)) => {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = sampler.await;
            return Err(format!("failed executing lk benchmark: {err}"));
        }
        Err(_) => {
            stop.store(true, std::sync::atomic::Ordering::Relaxed);
            let _ = sampler.await;
            return Err(format!(
                "lk benchmark timed out after {timeout:?} for {implementation}/{}",
                scenario.name
            ));
        }
    };

    stop.store(true, std::sync::atomic::Ordering::Relaxed);
    let samples = sampler
        .await
        .map_err(|err| format!("resource sampler task failed: {err}"))?;
    let summary = summarize_samples(&samples, ticks_per_second);
    let stdout = String::from_utf8_lossy(&output.stdout).to_string();
    let mut stderr = String::from_utf8_lossy(&output.stderr).to_string();
    let media_validation = validate_load_test_output(&stdout, scenario);
    if let Err(error) = &media_validation {
        if !stderr.is_empty() {
            stderr.push('\n');
        }
        stderr.push_str("load-test media validation failed: ");
        stderr.push_str(error);
    }

    Ok(ServerBenchmarkResult {
        status_success: output.status.success() && media_validation.is_ok(),
        stdout,
        stderr,
        summary,
    })
}

fn validate_load_test_output(
    output: &str,
    scenario: BenchmarkScenario,
) -> Result<(), String> {
    let expected_tracks = scenario.video_publishers + scenario.audio_publishers;
    if scenario.subscribers == 0 || expected_tracks == 0 {
        return Ok(());
    }

    let mut subscriber_track_rows = Vec::new();
    for line in output.lines() {
        let mut columns = line
            .split('│')
            .map(str::trim)
            .collect::<Vec<_>>();
        if columns.first().is_some_and(|column| column.is_empty()) {
            columns.remove(0);
        }
        if columns.len() < 4 || !columns[0].starts_with("Sub ") {
            continue;
        }
        let Some((received, expected)) = columns[1].split_once('/') else {
            continue;
        };
        let received = received.parse::<u16>().map_err(|_| {
            format!("{} has an invalid track summary {:?}", columns[0], columns[1])
        })?;
        let expected = expected.parse::<u16>().map_err(|_| {
            format!("{} has an invalid track summary {:?}", columns[0], columns[1])
        })?;
        subscriber_track_rows.push((columns[0].to_string(), received, expected));
    }
    let received_track_packets = received_track_packets(output);

    let expected_subscribers = usize::from(scenario.subscribers);
    if subscriber_track_rows.len() != expected_subscribers {
        return Err(format!(
            "expected {expected_subscribers} subscriber summaries, found {}",
            subscriber_track_rows.len()
        ));
    }

    for (subscriber, received, reported_expected) in subscriber_track_rows {
        if reported_expected != expected_tracks || received != expected_tracks {
            return Err(format!(
                "{subscriber} reports {received}/{reported_expected} tracks, expected {expected_tracks}/{expected_tracks}"
            ));
        }
        let track_packets = received_track_packets.get(&subscriber).map(Vec::as_slice).unwrap_or_default();
        if track_packets.len() != usize::from(expected_tracks) {
            return Err(format!(
                "{subscriber} has {} RTP track rows, expected {expected_tracks}",
                track_packets.len()
            ));
        }
        if let Some((track_sid, _)) = track_packets.iter().find(|(_, packets)| *packets == 0) {
            return Err(format!("{subscriber} track {track_sid} has no received packets"));
        }
    }

    Ok(())
}

fn ensure_comparable_packet_delivery(go_output: &str, oxidesfu_output: &str) -> Result<(), String> {
    let go_packets = received_packet_count(go_output)?;
    let oxidesfu_packets = received_packet_count(oxidesfu_output)?;
    if oxidesfu_packets.saturating_mul(2) < go_packets
        || go_packets.saturating_mul(2) < oxidesfu_packets
    {
        return Err(format!(
            "Go received {go_packets} RTP packets but Rust received {oxidesfu_packets}; expected totals within a 2x envelope"
        ));
    }
    Ok(())
}

fn received_packet_count(output: &str) -> Result<u64, String> {
    let packets = received_track_packets(output)
        .values()
        .flatten()
        .map(|(_, packets)| *packets)
        .sum::<u64>();
    if packets == 0 {
        return Err("no received RTP packets found in load-test output".to_string());
    }
    Ok(packets)
}

fn received_track_packets(output: &str) -> std::collections::HashMap<String, Vec<(String, u64)>> {
    let mut packets_by_subscriber = std::collections::HashMap::<String, Vec<(String, u64)>>::new();
    let mut current_subscriber = None::<String>;
    for line in output.lines() {
        let mut columns = line
            .split('│')
            .map(str::trim)
            .collect::<Vec<_>>();
        if columns.first().is_some_and(|column| column.is_empty()) {
            columns.remove(0);
        }
        if columns.len() < 4 {
            continue;
        }
        if columns[0].starts_with("Sub ") {
            current_subscriber = Some(columns[0].to_string());
        } else if !columns[0].is_empty() {
            current_subscriber = None;
        }
        if columns[1].contains('/') {
            current_subscriber = None;
            continue;
        }
        let Some(subscriber) = current_subscriber.as_ref() else {
            continue;
        };
        let Ok(packet_count) = columns[3].parse::<u64>() else {
            continue;
        };
        packets_by_subscriber
            .entry(subscriber.clone())
            .or_default()
            .push((columns[1].to_string(), packet_count));
    }
    packets_by_subscriber
}

async fn sample_process_resources_until_stopped(
    pid: u32,
    _ticks_per_second: u64,
    interval: Duration,
    stop: Arc<std::sync::atomic::AtomicBool>,
) -> Vec<ResourceSample> {
    let started = tokio::time::Instant::now();
    let mut samples = Vec::new();
    while !stop.load(std::sync::atomic::Ordering::Relaxed) {
        if let Some(sample) = read_resource_sample(pid, started.elapsed().as_millis()) {
            samples.push(sample);
        }
        tokio::time::sleep(interval).await;
    }
    if let Some(sample) = read_resource_sample(pid, started.elapsed().as_millis()) {
        samples.push(sample);
    }
    samples
}

fn read_resource_sample(pid: u32, elapsed_ms: u128) -> Option<ResourceSample> {
    let stat = std::fs::read_to_string(format!("/proc/{pid}/stat")).ok()?;
    let cpu_jiffies = parse_cpu_jiffies_from_proc_stat(&stat)?;
    let status = std::fs::read_to_string(format!("/proc/{pid}/status")).ok()?;
    let rss_kib = parse_vm_rss_kib_from_proc_status(&status).unwrap_or(0);
    let loadavg_1m = read_loadavg_1m().unwrap_or(0.0);
    let net_dev = std::fs::read_to_string(format!("/proc/{pid}/net/dev")).ok();
    let (net_rx_bytes, net_tx_bytes, net_rx_packets, net_tx_packets) = net_dev
        .as_deref()
        .and_then(parse_net_dev_totals)
        .map(|totals| {
            (
                Some(totals.rx_bytes),
                Some(totals.tx_bytes),
                Some(totals.rx_packets),
                Some(totals.tx_packets),
            )
        })
        .unwrap_or((None, None, None, None));
    let fd_count = count_open_fds_for_pid(pid);

    Some(ResourceSample {
        elapsed_ms,
        cpu_jiffies,
        rss_kib,
        loadavg_1m,
        net_rx_bytes,
        net_tx_bytes,
        net_rx_packets,
        net_tx_packets,
        fd_count,
    })
}

#[derive(Debug, Clone, Copy)]
struct NetDevTotals {
    rx_bytes: u64,
    tx_bytes: u64,
    rx_packets: u64,
    tx_packets: u64,
}

fn parse_net_dev_totals(contents: &str) -> Option<NetDevTotals> {
    let mut rx_bytes: u64 = 0;
    let mut tx_bytes: u64 = 0;
    let mut rx_packets: u64 = 0;
    let mut tx_packets: u64 = 0;
    let mut parsed_any = false;

    for line in contents.lines().skip(2) {
        let Some((_iface, values)) = line.split_once(':') else {
            continue;
        };
        let fields = values.split_whitespace().collect::<Vec<_>>();
        if fields.len() < 16 {
            continue;
        }

        let Some(cur_rx_bytes) = fields[0].parse::<u64>().ok() else {
            continue;
        };
        let Some(cur_rx_packets) = fields[1].parse::<u64>().ok() else {
            continue;
        };
        let Some(cur_tx_bytes) = fields[8].parse::<u64>().ok() else {
            continue;
        };
        let Some(cur_tx_packets) = fields[9].parse::<u64>().ok() else {
            continue;
        };

        rx_bytes = rx_bytes.saturating_add(cur_rx_bytes);
        rx_packets = rx_packets.saturating_add(cur_rx_packets);
        tx_bytes = tx_bytes.saturating_add(cur_tx_bytes);
        tx_packets = tx_packets.saturating_add(cur_tx_packets);
        parsed_any = true;
    }

    if parsed_any {
        Some(NetDevTotals {
            rx_bytes,
            tx_bytes,
            rx_packets,
            tx_packets,
        })
    } else {
        None
    }
}

fn count_open_fds_for_pid(pid: u32) -> Option<u64> {
    let entries = std::fs::read_dir(format!("/proc/{pid}/fd")).ok()?;
    Some(entries.count() as u64)
}

fn parse_cpu_jiffies_from_proc_stat(stat: &str) -> Option<u64> {
    let after_comm = stat.rsplit_once(") ")?.1;
    let fields = after_comm.split_whitespace().collect::<Vec<_>>();
    let utime = fields.get(11)?.parse::<u64>().ok()?;
    let stime = fields.get(12)?.parse::<u64>().ok()?;
    Some(utime.saturating_add(stime))
}

fn parse_vm_rss_kib_from_proc_status(status: &str) -> Option<u64> {
    for line in status.lines() {
        if let Some(rest) = line.strip_prefix("VmRSS:") {
            return rest.split_whitespace().next()?.parse::<u64>().ok();
        }
    }
    None
}

fn read_loadavg_1m() -> Option<f64> {
    let loadavg = std::fs::read_to_string("/proc/loadavg").ok()?;
    loadavg.split_whitespace().next()?.parse::<f64>().ok()
}

fn summarize_samples(samples: &[ResourceSample], ticks_per_second: u64) -> ResourceSummary {
    let first = samples.first().expect("benchmark should collect samples");
    let last = samples.last().expect("benchmark should collect samples");
    let wall_seconds = ((last.elapsed_ms.saturating_sub(first.elapsed_ms)) as f64 / 1_000.0).max(0.001);
    let cpu_jiffies = last.cpu_jiffies.saturating_sub(first.cpu_jiffies);
    let cpu_seconds = cpu_jiffies as f64 / ticks_per_second.max(1) as f64;
    let approx_cpu_percent_one_core = (cpu_seconds / wall_seconds) * 100.0;
    let peak_rss_kib = samples.iter().map(|sample| sample.rss_kib).max().unwrap_or(0);
    let avg_rss_kib = samples.iter().map(|sample| sample.rss_kib).sum::<u64>()
        / u64::try_from(samples.len()).expect("sample length should fit u64");
    let peak_loadavg_1m = samples
        .iter()
        .map(|sample| sample.loadavg_1m)
        .fold(0.0, f64::max);
    let avg_loadavg_1m = samples
        .iter()
        .map(|sample| sample.loadavg_1m)
        .sum::<f64>()
        / samples.len() as f64;
    let net_rx_bytes_delta = match (first.net_rx_bytes, last.net_rx_bytes) {
        (Some(a), Some(b)) => Some(b.saturating_sub(a)),
        _ => None,
    };
    let net_tx_bytes_delta = match (first.net_tx_bytes, last.net_tx_bytes) {
        (Some(a), Some(b)) => Some(b.saturating_sub(a)),
        _ => None,
    };
    let net_rx_packets_delta = match (first.net_rx_packets, last.net_rx_packets) {
        (Some(a), Some(b)) => Some(b.saturating_sub(a)),
        _ => None,
    };
    let net_tx_packets_delta = match (first.net_tx_packets, last.net_tx_packets) {
        (Some(a), Some(b)) => Some(b.saturating_sub(a)),
        _ => None,
    };
    let peak_fd_count = samples.iter().filter_map(|sample| sample.fd_count).max();

    ResourceSummary {
        sample_count: samples.len(),
        wall_seconds: round3(wall_seconds),
        cpu_seconds: round3(cpu_seconds),
        approx_cpu_percent_one_core: round3(approx_cpu_percent_one_core),
        peak_rss_kib,
        avg_rss_kib,
        peak_loadavg_1m: round3(peak_loadavg_1m),
        avg_loadavg_1m: round3(avg_loadavg_1m),
        net_rx_bytes_delta,
        net_tx_bytes_delta,
        net_rx_packets_delta,
        net_tx_packets_delta,
        peak_fd_count,
    }
}

fn clock_ticks_per_second() -> u64 {
    std::process::Command::new("getconf")
        .arg("CLK_TCK")
        .output()
        .ok()
        .and_then(|output| {
            if output.status.success() {
                String::from_utf8(output.stdout).ok()?.trim().parse().ok()
            } else {
                None
            }
        })
        .unwrap_or(100)
}

fn parse_duration_seconds(duration: &str) -> Option<u64> {
    duration.strip_suffix('s')?.parse().ok()
}

fn round3(value: f64) -> f64 {
    (value * 1_000.0).round() / 1_000.0
}

fn benchmark_include_log_tails() -> bool {
    std::env::var_os("OXIDESFU_BENCHMARK_INCLUDE_LOG_TAILS").is_some()
}

fn redact_base_url_for_artifact(implementation: &str) -> String {
    format!("redacted://{implementation}")
}

fn sanitize_artifact_text(value: &str) -> String {
    let mut sanitized = value.to_string();

    if let Ok(home) = std::env::var("HOME") {
        sanitized = sanitized.replace(&home, "~");
    }

    let workspace = oxidesfu_workspace_root().display().to_string();
    sanitized = sanitized.replace(&workspace, "<workspace>");

    sanitized
}

fn parse_mem_total_kib_from_meminfo(meminfo: &str) -> Option<u64> {
    for line in meminfo.lines() {
        if let Some(rest) = line.strip_prefix("MemTotal:") {
            return rest.split_whitespace().next()?.parse::<u64>().ok();
        }
    }
    None
}

fn parse_fd_limits_from_proc_limits(limits: &str) -> Option<(u64, u64)> {
    for line in limits.lines() {
        if !line.starts_with("Max open files") {
            continue;
        }
        let columns = line.split_whitespace().collect::<Vec<_>>();
        if columns.len() < 5 {
            return None;
        }
        let soft = columns.get(3)?.parse::<u64>().ok()?;
        let hard = columns.get(4)?.parse::<u64>().ok()?;
        return Some((soft, hard));
    }
    None
}

fn read_self_fd_limits() -> Option<(u64, u64)> {
    let limits = std::fs::read_to_string("/proc/self/limits").ok()?;
    parse_fd_limits_from_proc_limits(&limits)
}

fn read_git_revision() -> Option<String> {
    let output = std::process::Command::new("git")
        .arg("--no-pager")
        .arg("rev-parse")
        .arg("--short")
        .arg("HEAD")
        .current_dir(oxidesfu_workspace_root())
        .output()
        .ok()?;
    if !output.status.success() {
        return None;
    }
    Some(String::from_utf8_lossy(&output.stdout).trim().to_string())
}

fn environment_metadata() -> serde_json::Value {
    let logical_cpus = std::thread::available_parallelism()
        .map(usize::from)
        .unwrap_or(0);
    let rustc = std::process::Command::new("rustc")
        .arg("--version")
        .output()
        .ok()
        .and_then(|out| {
            if out.status.success() {
                Some(String::from_utf8_lossy(&out.stdout).trim().to_string())
            } else {
                None
            }
        })
        .unwrap_or_else(|| "unknown".to_string());
    let mem_total_kib = std::fs::read_to_string("/proc/meminfo")
        .ok()
        .and_then(|contents| parse_mem_total_kib_from_meminfo(&contents));
    let (fd_limit_soft, fd_limit_hard) = read_self_fd_limits().unwrap_or((0, 0));
    let git_revision = read_git_revision().unwrap_or_else(|| "unknown".to_string());

    serde_json::json!({
        "os": std::env::consts::OS,
        "arch": std::env::consts::ARCH,
        "logical_cpus": logical_cpus,
        "rustc": rustc,
        "memory_total_kib": mem_total_kib,
        "fd_limit_soft": fd_limit_soft,
        "fd_limit_hard": fd_limit_hard,
        "fd_limit_recommended": 65535u64,
        "fd_limit_meets_recommended": fd_limit_soft >= 65535,
        "git_revision": git_revision,
    })
}

fn metric_stats_json(stats: MetricStats) -> serde_json::Value {
    serde_json::json!({
        "min": stats.min,
        "median": stats.median,
        "p95": stats.p95,
        "max": stats.max,
        "mean": stats.mean,
    })
}

fn percent_delta(base: f64, current: f64) -> f64 {
    if base.abs() < f64::EPSILON {
        0.0
    } else {
        ((current - base) / base) * 100.0
    }
}

fn markdown_delta(base: f64, current: f64) -> String {
    format!("{:+.1}%", percent_delta(base, current))
}

fn p95_interpretation_note(runs: usize) -> String {
    let behavior = if runs <= 19 {
        "With <= 19 runs, nearest-rank p95 equals the maximum run value."
    } else {
        "With 20+ runs, nearest-rank p95 is still a high-order statistic and tail-sensitive."
    };
    format!(
        "p95 here is nearest-rank p95 over per-run aggregates (wall/cpu/peak RSS), not per-request or per-frame latency. {} Consider 50+ runs before treating p95 as a stable percentile estimate.",
        behavior
    )
}

fn implementation_display_name(implementation: &str) -> &'static str {
    match implementation {
        "go_livekit" => "LiveKit (Go)",
        "oxidesfu" => "OxideSFU (Rust)",
        _ => "Unknown",
    }
}

fn append_detailed_comparison_tables(
    markdown: &mut String,
    go: &ImplementationBenchmarkResult,
    oxidesfu: &ImplementationBenchmarkResult,
) {
    markdown.push_str("\n## Detailed median comparison (LiveKit (Go) baseline)\n\n");
    markdown.push_str("| Metric | LiveKit (Go) | OxideSFU (Rust) | Delta (OxideSFU (Rust) vs LiveKit (Go)) |\n");
    markdown.push_str("|---|---:|---:|---:|\n");
    markdown.push_str(&format!(
        "| Wall time (s) | {:.3} | {:.3} | {} |\n",
        go.aggregated.wall_seconds.median,
        oxidesfu.aggregated.wall_seconds.median,
        markdown_delta(
            go.aggregated.wall_seconds.median,
            oxidesfu.aggregated.wall_seconds.median,
        ),
    ));
    markdown.push_str(&format!(
        "| CPU time (s) | {:.3} | {:.3} | {} |\n",
        go.aggregated.cpu_seconds.median,
        oxidesfu.aggregated.cpu_seconds.median,
        markdown_delta(
            go.aggregated.cpu_seconds.median,
            oxidesfu.aggregated.cpu_seconds.median,
        ),
    ));
    markdown.push_str(&format!(
        "| Peak RSS (MiB) | {:.3} | {:.3} | {} |\n",
        go.aggregated.peak_rss_kib.median / 1024.0,
        oxidesfu.aggregated.peak_rss_kib.median / 1024.0,
        markdown_delta(
            go.aggregated.peak_rss_kib.median / 1024.0,
            oxidesfu.aggregated.peak_rss_kib.median / 1024.0,
        ),
    ));

    markdown.push_str("\n## Detailed p95 comparison (LiveKit (Go) baseline)\n\n");
    markdown.push_str("| Metric | LiveKit (Go) | OxideSFU (Rust) | Delta (OxideSFU (Rust) vs LiveKit (Go)) |\n");
    markdown.push_str("|---|---:|---:|---:|\n");
    markdown.push_str(&format!(
        "| Wall time p95 (s) | {:.3} | {:.3} | {} |\n",
        go.aggregated.wall_seconds.p95,
        oxidesfu.aggregated.wall_seconds.p95,
        markdown_delta(go.aggregated.wall_seconds.p95, oxidesfu.aggregated.wall_seconds.p95),
    ));
    markdown.push_str(&format!(
        "| CPU time p95 (s) | {:.3} | {:.3} | {} |\n",
        go.aggregated.cpu_seconds.p95,
        oxidesfu.aggregated.cpu_seconds.p95,
        markdown_delta(go.aggregated.cpu_seconds.p95, oxidesfu.aggregated.cpu_seconds.p95),
    ));
    markdown.push_str(&format!(
        "| Peak RSS p95 (MiB) | {:.3} | {:.3} | {} |\n",
        go.aggregated.peak_rss_kib.p95 / 1024.0,
        oxidesfu.aggregated.peak_rss_kib.p95 / 1024.0,
        markdown_delta(
            go.aggregated.peak_rss_kib.p95 / 1024.0,
            oxidesfu.aggregated.peak_rss_kib.p95 / 1024.0,
        ),
    ));
}

#[derive(Debug)]
struct OverviewRow {
    scenario_name: String,
    go_wall_median: f64,
    oxidesfu_wall_median: f64,
    go_cpu_median: f64,
    oxidesfu_cpu_median: f64,
    go_rss_median_mib: f64,
    oxidesfu_rss_median_mib: f64,
    go_wall_p95: f64,
    oxidesfu_wall_p95: f64,
    go_cpu_p95: f64,
    oxidesfu_cpu_p95: f64,
    go_rss_p95_mib: f64,
    oxidesfu_rss_p95_mib: f64,
}

fn parse_overview_row_from_json(path: &std::path::Path) -> Option<OverviewRow> {
    let raw = std::fs::read_to_string(path).ok()?;
    let value: serde_json::Value = serde_json::from_str(&raw).ok()?;

    let scenario_name = value.get("scenario")?.get("name")?.as_str()?.to_string();
    let results = value.get("results")?.as_array()?;

    let metric = |implementation: &str, bucket: &str, field: &str| -> Option<f64> {
        let result = results
            .iter()
            .find(|entry| entry.get("implementation").and_then(|v| v.as_str()) == Some(implementation))?;
        result
            .get("aggregated")?
            .get(bucket)?
            .get(field)?
            .as_f64()
    };

    Some(OverviewRow {
        scenario_name,
        go_wall_median: metric("go_livekit", "wall_seconds", "median")?,
        oxidesfu_wall_median: metric("oxidesfu", "wall_seconds", "median")?,
        go_cpu_median: metric("go_livekit", "cpu_seconds", "median")?,
        oxidesfu_cpu_median: metric("oxidesfu", "cpu_seconds", "median")?,
        go_rss_median_mib: metric("go_livekit", "peak_rss_kib", "median")? / 1024.0,
        oxidesfu_rss_median_mib: metric("oxidesfu", "peak_rss_kib", "median")? / 1024.0,
        go_wall_p95: metric("go_livekit", "wall_seconds", "p95")?,
        oxidesfu_wall_p95: metric("oxidesfu", "wall_seconds", "p95")?,
        go_cpu_p95: metric("go_livekit", "cpu_seconds", "p95")?,
        oxidesfu_cpu_p95: metric("oxidesfu", "cpu_seconds", "p95")?,
        go_rss_p95_mib: metric("go_livekit", "peak_rss_kib", "p95")? / 1024.0,
        oxidesfu_rss_p95_mib: metric("oxidesfu", "peak_rss_kib", "p95")? / 1024.0,
    })
}

fn update_benchmark_overview_artifact(artifact_dir: &std::path::Path) -> std::io::Result<()> {
    let mut latest_paths_by_scenario = std::collections::BTreeMap::<String, std::path::PathBuf>::new();

    for entry in std::fs::read_dir(artifact_dir)? {
        let entry = entry?;
        let path = entry.path();
        if path.extension().and_then(|ext| ext.to_str()) != Some("json") {
            continue;
        }

        let Some(stem) = path.file_stem().and_then(|s| s.to_str()) else {
            continue;
        };
        let Some((scenario, _suffix)) = stem.rsplit_once('-') else {
            continue;
        };
        if scenario == "overview" {
            continue;
        }

        let scenario_key = scenario.to_string();
        if let Some(previous) = latest_paths_by_scenario.get(&scenario_key) {
            let prev_name = previous.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            let cur_name = path.file_name().and_then(|n| n.to_str()).unwrap_or_default();
            if cur_name > prev_name {
                latest_paths_by_scenario.insert(scenario_key, path);
            }
        } else {
            latest_paths_by_scenario.insert(scenario_key, path);
        }
    }

    let mut rows = latest_paths_by_scenario
        .values()
        .filter_map(|path| parse_overview_row_from_json(path.as_path()))
        .collect::<Vec<_>>();
    rows.sort_by(|a, b| a.scenario_name.cmp(&b.scenario_name));

    let mut markdown = String::from("# OxideSFU benchmark overview (latest per scenario)\n\n");
    markdown.push_str("## Median comparison\n\n");
    markdown.push_str(
        "| Scenario | Wall LiveKit (Go)→OxideSFU (Rust) (s) | Δ Wall | CPU LiveKit (Go)→OxideSFU (Rust) (s) | Δ CPU | Peak RSS LiveKit (Go)→OxideSFU (Rust) (MiB) | Δ RSS |\n",
    );
    markdown.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    for row in &rows {
        markdown.push_str(&format!(
            "| {} | {:.3} → {:.3} | {} | {:.3} → {:.3} | {} | {:.3} → {:.3} | {} |\n",
            row.scenario_name,
            row.go_wall_median,
            row.oxidesfu_wall_median,
            markdown_delta(row.go_wall_median, row.oxidesfu_wall_median),
            row.go_cpu_median,
            row.oxidesfu_cpu_median,
            markdown_delta(row.go_cpu_median, row.oxidesfu_cpu_median),
            row.go_rss_median_mib,
            row.oxidesfu_rss_median_mib,
            markdown_delta(row.go_rss_median_mib, row.oxidesfu_rss_median_mib),
        ));
    }

    markdown.push_str("\n## P95 comparison\n\n");
    markdown.push_str(
        "| Scenario | Wall p95 LiveKit (Go)→OxideSFU (Rust) (s) | Δ Wall p95 | CPU p95 LiveKit (Go)→OxideSFU (Rust) (s) | Δ CPU p95 | Peak RSS p95 LiveKit (Go)→OxideSFU (Rust) (MiB) | Δ RSS p95 |\n",
    );
    markdown.push_str("|---|---:|---:|---:|---:|---:|---:|\n");
    for row in &rows {
        markdown.push_str(&format!(
            "| {} | {:.3} → {:.3} | {} | {:.3} → {:.3} | {} | {:.3} → {:.3} | {} |\n",
            row.scenario_name,
            row.go_wall_p95,
            row.oxidesfu_wall_p95,
            markdown_delta(row.go_wall_p95, row.oxidesfu_wall_p95),
            row.go_cpu_p95,
            row.oxidesfu_cpu_p95,
            markdown_delta(row.go_cpu_p95, row.oxidesfu_cpu_p95),
            row.go_rss_p95_mib,
            row.oxidesfu_rss_p95_mib,
            markdown_delta(row.go_rss_p95_mib, row.oxidesfu_rss_p95_mib),
        ));
    }

    markdown.push_str("\nGenerated automatically from latest scenario artifacts in this directory.\n");

    std::fs::write(artifact_dir.join("overview.md"), markdown)
}

fn write_benchmark_artifacts(
    config: BenchmarkConfig,
    scenario: BenchmarkScenario,
    results: &[&ImplementationBenchmarkResult],
) -> std::io::Result<PathBuf> {
    let artifact_dir = oxidesfu_workspace_root().join("target/benchmarks");
    std::fs::create_dir_all(&artifact_dir)?;
    let stem = format!("{}-{}", scenario.name, unique_suffix());
    let json_path = artifact_dir.join(format!("{stem}.json"));
    let md_path = artifact_dir.join(format!("{stem}.md"));

    let include_log_tails = benchmark_include_log_tails();
    let json_results = results
        .iter()
        .map(|result| {
            let run_summaries = result
                .runs
                .iter()
                .map(|run| {
                    serde_json::json!({
                        "status_success": run.status_success,
                        "sample_count": run.summary.sample_count,
                        "wall_seconds": run.summary.wall_seconds,
                        "cpu_seconds": run.summary.cpu_seconds,
                        "approx_cpu_percent_one_core": run.summary.approx_cpu_percent_one_core,
                        "peak_rss_kib": run.summary.peak_rss_kib,
                        "avg_rss_kib": run.summary.avg_rss_kib,
                        "peak_loadavg_1m": run.summary.peak_loadavg_1m,
                        "avg_loadavg_1m": run.summary.avg_loadavg_1m,
                        "net_rx_bytes_delta": run.summary.net_rx_bytes_delta,
                        "net_tx_bytes_delta": run.summary.net_tx_bytes_delta,
                        "net_rx_packets_delta": run.summary.net_rx_packets_delta,
                        "net_tx_packets_delta": run.summary.net_tx_packets_delta,
                        "peak_fd_count": run.summary.peak_fd_count,
                    })
                })
                .collect::<Vec<_>>();
            let mut payload = serde_json::json!({
                "implementation": result.implementation,
                "implementation_display": implementation_display_name(result.implementation),
                "base_url": redact_base_url_for_artifact(result.implementation),
                "status_success": result.status_success,
                "successful_runs": result.successful_runs,
                "total_runs": result.total_runs,
                "aggregated": {
                    "wall_seconds": metric_stats_json(result.aggregated.wall_seconds),
                    "cpu_seconds": metric_stats_json(result.aggregated.cpu_seconds),
                    "approx_cpu_percent_one_core": metric_stats_json(result.aggregated.approx_cpu_percent_one_core),
                    "peak_rss_kib": metric_stats_json(result.aggregated.peak_rss_kib),
                },
                "run_summaries": run_summaries,
            });

            if include_log_tails {
                let stdout = result
                    .runs
                    .iter()
                    .map(|run| run.stdout.as_str())
                    .collect::<Vec<_>>()
                    .join("\n\n--- run separator ---\n\n");
                let stderr = result
                    .runs
                    .iter()
                    .map(|run| run.stderr.as_str())
                    .collect::<Vec<_>>()
                    .join("\n\n--- run separator ---\n\n");
                payload["stdout_tail"] =
                    serde_json::json!(tail_chars(&sanitize_artifact_text(&stdout), 12_000));
                payload["stderr_tail"] =
                    serde_json::json!(tail_chars(&sanitize_artifact_text(&stderr), 12_000));
            }

            payload
        })
        .collect::<Vec<_>>();

    let p95_note = p95_interpretation_note(config.runs);

    let document = serde_json::json!({
        "privacy": {
            "base_url": "redacted",
            "log_tails_included": include_log_tails,
            "log_tails_opt_in_env": "OXIDESFU_BENCHMARK_INCLUDE_LOG_TAILS=1"
        },
        "notes": {
            "p95": p95_note
        },
        "config": {
            "mode": format!("{:?}", config.mode),
            "runs": config.runs,
            "max_wall_regression_percent": config.max_wall_regression_percent,
            "max_cpu_regression_percent": config.max_cpu_regression_percent,
            "max_peak_rss_regression_percent": config.max_peak_rss_regression_percent,
        },
        "environment": environment_metadata(),
        "scenario": {
            "name": scenario.name,
            "duration": scenario.duration,
            "video_publishers": scenario.video_publishers,
            "audio_publishers": scenario.audio_publishers,
            "subscribers": scenario.subscribers,
            "num_per_second": scenario.num_per_second,
            "layout": scenario.layout,
            "video_resolution": scenario.video_resolution,
            "video_codec": BENCHMARK_VIDEO_CODEC,
            "no_simulcast": scenario.no_simulcast,
        },
        "results": json_results,
    });
    std::fs::write(&json_path, serde_json::to_vec_pretty(&document)?)?;

    let mut markdown = format!("# OxideSFU benchmark comparison: {}\n\n", scenario.name);
    markdown.push_str(&format!(
        "Mode: `{:?}` · Runs: `{}` · Gates: wall `{:.1}%`, cpu `{:.1}%`, peak RSS `{:.1}%`\n\n",
        config.mode,
        config.runs,
        config.max_wall_regression_percent,
        config.max_cpu_regression_percent,
        config.max_peak_rss_regression_percent,
    ));
    markdown.push_str("| Implementation | Success | Wall median s | Wall p95 s | CPU median s | CPU p95 s | Peak RSS median MiB | Peak RSS p95 MiB |\n");
    markdown.push_str("|---|---:|---:|---:|---:|---:|---:|---:|\n");
    for result in results {
        markdown.push_str(&format!(
            "| {} | {}/{} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} | {:.3} |\n",
            implementation_display_name(result.implementation),
            result.successful_runs,
            result.total_runs,
            result.aggregated.wall_seconds.median,
            result.aggregated.wall_seconds.p95,
            result.aggregated.cpu_seconds.median,
            result.aggregated.cpu_seconds.p95,
            result.aggregated.peak_rss_kib.median / 1024.0,
            result.aggregated.peak_rss_kib.p95 / 1024.0,
        ));
    }

    if let (Some(go), Some(oxidesfu)) = (
        results
            .iter()
            .find(|result| result.implementation == "go_livekit"),
        results
            .iter()
            .find(|result| result.implementation == "oxidesfu"),
    ) {
        append_detailed_comparison_tables(&mut markdown, go, oxidesfu);
    }

    markdown.push_str(&format!("\nInterpretation note: {}\n", p95_interpretation_note(config.runs)));
    markdown.push_str("\nArtifacts are comparison evidence, not absolute capacity certification. Run on an otherwise idle host for meaningful CPU/RSS comparisons.\n");
    std::fs::write(&md_path, markdown)?;
    update_benchmark_overview_artifact(artifact_dir.as_path())?;

    Ok(artifact_dir)
}

fn tail_chars(value: &str, max_chars: usize) -> String {
    let char_count = value.chars().count();
    if char_count <= max_chars {
        return value.to_string();
    }
    value.chars().skip(char_count - max_chars).collect()
}
