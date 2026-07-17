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

#[tokio::test]
async fn livekit_yaml_redis_process_supports_room_api_and_join() {
    let Some((mut redis, redis_url)) = spawn_ready_redis_server_for_distributed_tests().await else {
        eprintln!("skipping YAML Redis process contract because Redis is unavailable");
        return;
    };
    let reservation = tokio::net::TcpListener::bind("127.0.0.1:0").await.expect("port reservation should bind");
    let port = reservation.local_addr().expect("reservation should expose address").port();
    drop(reservation);
    let redis_address = redis_url
        .strip_prefix("redis://")
        .and_then(|value| value.strip_suffix("/0"))
        .expect("test Redis URL should have the expected shape");
    let config_path = std::env::temp_dir().join(format!("oxidesfu-livekit-yaml-redis-{}-{}.yaml", std::process::id(), unique_suffix()));
    std::fs::write(&config_path, format!("port: {port}\nkeys:\n  {API_KEY}: {API_SECRET}\nredis:\n  address: {redis_address}\nroom:\n  auto_create: true\n")).expect("Redis YAML fixture should write");
    let (mut server, url) = spawn_oxidesfu_server_process_with_livekit_config(port, &config_path)
        .await.expect("YAML Redis server should start").expect("server binary should be available");
    let room = format!("yaml-redis-{}", unique_suffix());
    let client = RoomClient::with_api_key(&url, API_KEY, API_SECRET).with_failover(false).with_request_timeout(Duration::from_secs(5));
    client.create_room(&room, CreateRoomOptions::default()).await.expect("YAML Redis room should create");
    assert!(client.list_rooms(Vec::new()).await.expect("YAML Redis room list should succeed").iter().any(|candidate| candidate.name == room));
    let join = run_signal_join_participant_visibility(&url, &room, "yaml-redis-alice", "YAML Redis Alice").await;
    assert_eq!(join.joined_room_name, room);
    client.delete_room(&room).await.expect("YAML Redis room should delete");
    let _ = server.kill().await;
    let _ = redis.kill().await;
    let _ = std::fs::remove_file(config_path);
}
