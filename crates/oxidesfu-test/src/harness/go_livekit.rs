    fn go_livekit_server_binary_path() -> std::path::PathBuf {
        std::env::temp_dir().join("oxidesfu-livekit-server-harness")
    }

    fn ensure_go_livekit_server_binary() -> Result<Option<std::path::PathBuf>, String> {
        const GO_NOT_FOUND: &str = "GO_NOT_FOUND";
        static BUILD_RESULT: std::sync::OnceLock<Result<std::path::PathBuf, String>> =
            std::sync::OnceLock::new();

        let result = BUILD_RESULT.get_or_init(|| {
            let binary_path = go_livekit_server_binary_path();
            let mut build = std::process::Command::new("go");
            build
                .arg("build")
                .arg("-o")
                .arg(&binary_path)
                .arg("./cmd/server")
                .current_dir("/home/andre/rustprojects/othercode/livekit");

            match build.output() {
                Ok(output) => {
                    if output.status.success() {
                        Ok(binary_path)
                    } else {
                        let stderr = String::from_utf8_lossy(&output.stderr);
                        Err(format!(
                            "failed building go livekit server binary for harness: status={} stderr={}",
                            output.status,
                            stderr.trim()
                        ))
                    }
                }
                Err(err) if err.kind() == ErrorKind::NotFound => Err(GO_NOT_FOUND.to_string()),
                Err(err) => Err(format!("failed invoking go build for harness: {err}")),
            }
        });

        match result {
            Ok(path) => Ok(Some(path.clone())),
            Err(err) if err == GO_NOT_FOUND => Ok(None),
            Err(err) => Err(err.clone()),
        }
    }

    async fn spawn_go_livekit_server(
        http_port: u16,
        rtc_tcp_port: u16,
        rtc_udp_port: u16,
    ) -> Result<Option<tokio::process::Child>, String> {
        let Some(binary_path) = ensure_go_livekit_server_binary()? else {
            return Ok(None);
        };

        let mut command = tokio::process::Command::new(binary_path);
        command.kill_on_drop(true);
        let http_port_arg = http_port.to_string();
        let rtc_tcp_port_arg = rtc_tcp_port.to_string();
        let rtc_udp_port_arg = rtc_udp_port.to_string();
        command
            .arg("--dev")
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(&http_port_arg)
            .arg("--rtc.tcp_port")
            .arg(&rtc_tcp_port_arg)
            .arg("--udp-port")
            .arg(&rtc_udp_port_arg)
            .arg("--keys")
            .arg("devkey: secret")
            .current_dir("/home/andre/rustprojects/othercode/livekit")
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        match command.spawn() {
            Ok(child) => Ok(Some(child)),
            Err(err) if err.kind() == ErrorKind::NotFound => Ok(None),
            Err(err) => Err(format!("failed to spawn go livekit server: {err}")),
        }
    }
    #[test]
    fn go_spawn_warmup_delay_is_small_and_bounded() {
        let delay = go_spawn_warmup_delay();
        assert!(delay >= Duration::from_millis(100));
        assert!(delay <= Duration::from_secs(2));
    }
    fn go_spawn_warmup_delay() -> Duration {
        Duration::from_millis(175)
    }
    #[test]
    fn readiness_respawn_backoff_is_bounded_and_monotonic() {
        let first = readiness_respawn_backoff(1);
        let second = readiness_respawn_backoff(2);
        let tenth = readiness_respawn_backoff(10);
        let maxed = readiness_respawn_backoff(40);

        assert_eq!(first, Duration::from_millis(125));
        assert_eq!(second, Duration::from_millis(200));
        assert!(second >= first);
        assert_eq!(tenth, Duration::from_millis(800));
        assert_eq!(maxed, Duration::from_secs(2));
    }
    fn readiness_respawn_backoff(attempt: usize) -> Duration {
        let attempt_u64 = u64::try_from(attempt).unwrap_or(u64::MAX);
        let millis = 50u64.saturating_add(attempt_u64.saturating_mul(75));
        Duration::from_millis(millis.min(2_000))
    }
    fn readiness_failure_looks_retryable(err: &str, exit_hint: &str) -> bool {
        err.contains("process exited early")
            || exit_hint.contains("child already exited with status")
            || err.contains("connection refused")
            || err.contains("deadline has elapsed")
            || err.contains("timed out")
    }
    async fn spawn_ready_go_livekit_server_with_single_respawn()
    -> Result<Option<(tokio::process::Child, String)>, String> {
        const MAX_ATTEMPTS: usize = 20;

        for attempt in 1..=MAX_ATTEMPTS {
            let go_http_port = reserve_local_port();
            let go_tcp_port = reserve_local_port();
            let go_udp_port = reserve_local_port();

            let Some(mut go_livekit) =
                spawn_go_livekit_server(go_http_port, go_tcp_port, go_udp_port).await?
            else {
                return Ok(None);
            };

            let go_base_url = format!("http://127.0.0.1:{go_http_port}");
            tokio::time::sleep(go_spawn_warmup_delay()).await;
            match wait_for_room_service_ready_with_retry_and_process(
                &go_base_url,
                Duration::from_secs(180),
                Duration::from_millis(150),
                Duration::from_secs(2),
                Some(&mut go_livekit),
            )
            .await
            {
                Ok(()) => return Ok(Some((go_livekit, go_base_url))),
                Err(err) => {
                    let exit_hint = match go_livekit.try_wait() {
                        Ok(Some(status)) => format!("child already exited with status: {status}"),
                        Ok(None) => "child still running but not ready".to_string(),
                        Err(wait_err) => {
                            format!(
                                "failed to inspect child status after readiness failure: {wait_err}"
                            )
                        }
                    };
                    let _ = go_livekit.kill().await;

                    if attempt < MAX_ATTEMPTS && readiness_failure_looks_retryable(&err, &exit_hint) {
                        tokio::time::sleep(readiness_respawn_backoff(attempt)).await;
                        continue;
                    }

                    return Err(format!(
                        "go livekit server readiness failed after {attempt} attempts: {err}; {exit_hint}"
                    ));
                }
            }
        }

        Err("go livekit server readiness failed after exhausting attempts".to_string())
    }
    async fn wait_for_room_service_ready(base_url: &str) -> Result<(), String> {
        wait_for_room_service_ready_with_retry(
            base_url,
            Duration::from_secs(180),
            Duration::from_millis(150),
            Duration::from_secs(2),
        )
        .await
    }
    async fn wait_for_room_service_ready_with_retry(
        base_url: &str,
        max_wait: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
    ) -> Result<(), String> {
        wait_for_room_service_ready_with_retry_and_process(
            base_url,
            max_wait,
            initial_backoff,
            max_backoff,
            None,
        )
        .await
    }
    async fn wait_for_room_service_ready_with_retry_and_process(
        base_url: &str,
        max_wait: Duration,
        initial_backoff: Duration,
        max_backoff: Duration,
        mut process: Option<&mut tokio::process::Child>,
    ) -> Result<(), String> {
        let client = RoomClient::with_api_key(base_url, API_KEY, API_SECRET)
            .with_failover(false)
            .with_request_timeout(Duration::from_secs(1));

        let deadline = tokio::time::Instant::now() + max_wait;
        let mut backoff = initial_backoff.max(Duration::from_millis(10));
        let max_backoff = max_backoff.max(Duration::from_millis(10));
        if backoff > max_backoff {
            backoff = max_backoff;
        }
        let last_err = loop {
            if let Some(child) = process.as_deref_mut() {
                match child.try_wait() {
                    Ok(Some(status)) => {
                        break format!(
                            "process exited early while waiting for readiness: {status}"
                        );
                    }
                    Ok(None) => {}
                    Err(err) => {
                        break format!(
                            "failed to inspect process status while waiting for readiness: {err}"
                        );
                    }
                }
            }

            match client.list_rooms(Vec::new()).await {
                Ok(_) => return Ok(()),
                Err(err) => {
                    if tokio::time::Instant::now() >= deadline {
                        break err.to_string();
                    }
                    tokio::time::sleep(backoff).await;
                    backoff = backoff
                        .checked_mul(2)
                        .unwrap_or(max_backoff)
                        .min(max_backoff);
                }
            }
        };

        Err(format!(
            "room service did not become ready at {base_url} within {max_wait:?}: {last_err}"
        ))
    }
