    #[derive(Debug, Clone, Default)]
    struct OxidesfuServerProcessOptions {
        rtc_udp_port: Option<u16>,
        rtc_udp_port_range_start: Option<u16>,
        rtc_udp_port_range_end: Option<u16>,
        rtc_tcp_port: Option<u16>,
        rtc_use_external_ip: Option<bool>,
        rtc_node_ip: Option<String>,
    }

    async fn spawn_oxidesfu_server_process(
        bind_port: u16,
        redis_url: &str,
        reject_non_local_room_placement: bool,
    ) -> Result<Option<(tokio::process::Child, String)>, String> {
        spawn_oxidesfu_server_process_with_options(
            bind_port,
            redis_url,
            reject_non_local_room_placement,
            &OxidesfuServerProcessOptions::default(),
        )
        .await
    }

    async fn spawn_oxidesfu_server_process_with_options(
        bind_port: u16,
        redis_url: &str,
        reject_non_local_room_placement: bool,
        options: &OxidesfuServerProcessOptions,
    ) -> Result<Option<(tokio::process::Child, String)>, String> {
        if !ensure_oxidesfu_server_binary_built().await? {
            return Ok(None);
        }

        let binary_path = oxidesfu_server_binary_path();
        let bind = format!("127.0.0.1:{bind_port}");
        let mut command = tokio::process::Command::new(&binary_path);
        command.kill_on_drop(true);
        command
            .arg("--bind")
            .arg(&bind)
            .arg("--api-key")
            .arg(API_KEY)
            .arg("--api-secret")
            .arg(API_SECRET)
            .arg("--room-node-directory-backend")
            .arg("redis")
            .arg("--redis-url")
            .arg(redis_url)
            .arg("--reject-non-local-room-placement")
            .arg(if reject_non_local_room_placement {
                "true"
            } else {
                "false"
            });

        if let Some(rtc_udp_port) = options.rtc_udp_port {
            command.arg("--rtc-udp-port").arg(rtc_udp_port.to_string());
        }
        if let Some(rtc_udp_port_range_start) = options.rtc_udp_port_range_start {
            command
                .arg("--rtc-udp-port-range-start")
                .arg(rtc_udp_port_range_start.to_string());
        }
        if let Some(rtc_udp_port_range_end) = options.rtc_udp_port_range_end {
            command
                .arg("--rtc-udp-port-range-end")
                .arg(rtc_udp_port_range_end.to_string());
        }
        if let Some(rtc_tcp_port) = options.rtc_tcp_port {
            command.arg("--rtc-tcp-port").arg(rtc_tcp_port.to_string());
        }
        if let Some(rtc_use_external_ip) = options.rtc_use_external_ip {
            command
                .arg("--rtc-use-external-ip")
                .arg(if rtc_use_external_ip { "true" } else { "false" });
        }
        if let Some(rtc_node_ip) = options.rtc_node_ip.as_ref() {
            command.arg("--rtc-node-ip").arg(rtc_node_ip);
        }

        command.current_dir(oxidesfu_workspace_root());

        if std::env::var_os("OXIDESFU_TEST_SERVER_STDIO").is_some() {
            command.stdout(Stdio::inherit()).stderr(Stdio::inherit());
        } else {
            command.stdout(Stdio::null()).stderr(Stdio::null());
        }

        let mut child = command
            .spawn()
            .map_err(|err| format!("failed to spawn oxidesfu-server process: {err}"))?;

        let base_url = format!("http://127.0.0.1:{bind_port}");
        match wait_for_room_service_ready_with_retry_and_process(
            &base_url,
            Duration::from_secs(45),
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
                    "oxidesfu-server process did not become ready: {err}"
                ))
            }
        }
    }
    fn oxidesfu_server_binary_path() -> PathBuf {
        oxidesfu_workspace_root().join("target/debug/oxidesfu-server")
    }
    fn oxidesfu_workspace_root() -> PathBuf {
        Path::new(env!("CARGO_MANIFEST_DIR"))
            .ancestors()
            .nth(2)
            .expect("workspace root should be two ancestors above oxidesfu-test manifest")
            .to_path_buf()
    }
    fn assert_success(output: Output, context: &str) {
        if !output.status.success() {
            panic!(
                "{context}\nstdout:\n{}\nstderr:\n{}",
                String::from_utf8_lossy(&output.stdout),
                String::from_utf8_lossy(&output.stderr)
            );
        }
    }
