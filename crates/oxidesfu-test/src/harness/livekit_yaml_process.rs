#[tokio::test]
async fn livekit_yaml_startup_process_supports_cli_room_lifecycle() {
    let Some(version) = run_lk(["--version"], None).await else {
        eprintln!("skipping YAML process smoke test because lk is not on PATH");
        return;
    };
    assert_success(version, "lk --version should run");

    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0")
        .await
        .expect("test listener should reserve a port");
    let port = reservation
        .local_addr()
        .expect("reserved listener should expose its address")
        .port();
    drop(reservation);

    let config_path = std::env::temp_dir().join(format!(
        "oxidesfu-livekit-yaml-{}-{}.yaml",
        std::process::id(),
        unique_suffix()
    ));
    std::fs::write(
        &config_path,
        format!(
            "port: {port}\nkeys:\n  {API_KEY}: {API_SECRET}\nroom:\n  auto_create: true\n"
        ),
    )
    .expect("test YAML fixture should write");

    let (mut server, url) = spawn_oxidesfu_server_process_with_livekit_config(port, &config_path)
        .await
        .expect("YAML-configured server should start")
        .expect("server binary should be available");
    let room = format!("yaml-cli-smoke-{}", unique_suffix());
    let common = [
        "--url",
        url.as_str(),
        "--api-key",
        API_KEY,
        "--api-secret",
        API_SECRET,
        "--yes",
    ];

    let create = run_lk(
        common.into_iter().chain(["room", "create", room.as_str()]),
        None,
    )
    .await
    .expect("lk was available during version check");
    assert_success(create, "lk room create should use YAML credentials");

    let list = run_lk(
        common
            .into_iter()
            .chain(["room", "list", "--json", room.as_str()]),
        None,
    )
    .await
    .expect("lk was available during version check");
    let stdout = String::from_utf8_lossy(&list.stdout).to_string();
    assert_success(list, "lk room list should use YAML credentials");
    assert!(
        stdout.contains(&room),
        "lk room list should include YAML-configured room {room}; stdout: {stdout}"
    );

    let delete = run_lk(
        common.into_iter().chain(["room", "delete", room.as_str()]),
        None,
    )
    .await
    .expect("lk was available during version check");
    assert_success(delete, "lk room delete should use YAML credentials");

    let _ = server.kill().await;
    let _ = std::fs::remove_file(config_path);
}
