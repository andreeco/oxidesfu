    #[derive(Default)]
    struct RecordingRelayDispatcher {
        intents: Mutex<Vec<NonLocalRelayJoinIntent>>,
    }
    #[derive(Default, Clone)]
    struct InMemoryRedisHashStore {
        values: Arc<Mutex<HashMap<(String, String), String>>>,
    }
    impl RedisHashStore for InMemoryRedisHashStore {
        fn hset(&self, key: &str, field: &str, value: &str) -> Result<(), RoomNodeRegistryError> {
            self.values
                .lock()
                .expect("redis hash store lock should not be poisoned")
                .insert((key.to_string(), field.to_string()), value.to_string());
            Ok(())
        }

        fn hget(&self, key: &str, field: &str) -> Result<Option<String>, RoomNodeRegistryError> {
            Ok(self
                .values
                .lock()
                .expect("redis hash store lock should not be poisoned")
                .get(&(key.to_string(), field.to_string()))
                .cloned())
        }

        fn hdel(&self, key: &str, field: &str) -> Result<(), RoomNodeRegistryError> {
            self.values
                .lock()
                .expect("redis hash store lock should not be poisoned")
                .remove(&(key.to_string(), field.to_string()));
            Ok(())
        }

        fn hvals(&self, key: &str) -> Result<Vec<String>, RoomNodeRegistryError> {
            let values = self
                .values
                .lock()
                .expect("redis hash store lock should not be poisoned")
                .iter()
                .filter_map(|((k, _), value)| (k == key).then_some(value.clone()))
                .collect();
            Ok(values)
        }
    }
    #[derive(Clone)]
    struct RemoteRoomStoreExecutionDriver {
        remote_rooms: oxidesfu_room::RoomStore,
    }
    impl RelayIntentExecutionDriver<InMemoryRedisHashStore> for RemoteRoomStoreExecutionDriver {
        fn drive_for_node(
            &self,
            mailbox: &RedisRelayMailbox<InMemoryRedisHashStore>,
            selected_room_node_id: &str,
        ) -> Result<(), String> {
            while let Some((receipt, intent)) = mailbox
                .claim_next_intent_for_node(selected_room_node_id)
                .map_err(|err| err.to_string())?
            {
                let (_room, participant, _others) = self
                    .remote_rooms
                    .join_participant(
                        &intent.room,
                        &intent.identity,
                        &intent.name,
                        String::new(),
                        HashMap::new(),
                    )
                    .map_err(|err| err.to_string())?;
                mailbox
                    .store_response(
                        &receipt,
                        &NonLocalRelayJoinResponse::Accepted {
                            participant_sid: participant.sid,
                            server_version: "relay-remote-room-store".to_string(),
                            ping_interval: 5,
                            ping_timeout: 15,
                        },
                    )
                    .map_err(|err| err.to_string())?;
            }
            Ok(())
        }

        fn drive_termination_for_node(
            &self,
            mailbox: &RedisRelayMailbox<InMemoryRedisHashStore>,
            selected_room_node_id: &str,
        ) -> Result<(), String> {
            while let Some(intent) = mailbox
                .claim_next_termination_intent_for_node(selected_room_node_id)
                .map_err(|err| err.to_string())?
            {
                let _ = self
                    .remote_rooms
                    .remove_participant(&intent.room, &intent.identity);
            }
            Ok(())
        }

        fn drive_room_service_requests_for_node(
            &self,
            mailbox: &RedisRelayMailbox<InMemoryRedisHashStore>,
            selected_room_node_id: &str,
        ) -> Result<(), String> {
            while let Some((receipt, intent)) = mailbox
                .claim_next_room_service_intent_for_node(selected_room_node_id)
                .map_err(|err| err.to_string())?
            {
                let response = match intent.method.as_str() {
                    "ListRooms" => {
                        let request = proto::ListRoomsRequest::decode(intent.request.as_slice())
                            .map_err(|err| err.to_string())?;
                        match self.remote_rooms.list_rooms(&request.names) {
                            Ok(rooms) => {
                                let rooms = rooms
                                    .into_iter()
                                    .map(|room| {
                                        proto::Room::decode(room.encode_to_vec().as_slice())
                                            .map_err(|err| err.to_string())
                                    })
                                    .collect::<Result<Vec<_>, _>>()?;
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success {
                                    response: proto::ListRoomsResponse { rooms }.encode_to_vec(),
                                }
                            }
                            Err(err) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 404,
                                    code: "not_found".to_string(),
                                    msg: err.to_string(),
                                }
                            }
                        }
                    }
                    "GetParticipant" => {
                        let request = proto::RoomParticipantIdentity::decode(intent.request.as_slice())
                            .map_err(|err| err.to_string())?;
                        match self
                            .remote_rooms
                            .get_participant(&request.room, &request.identity)
                        {
                            Ok(participant) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success {
                                    response: participant.encode_to_vec(),
                                }
                            }
                            Err(err) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 404,
                                    code: "not_found".to_string(),
                                    msg: err.to_string(),
                                }
                            }
                        }
                    }
                    "ListParticipants" => {
                        let request = proto::ListParticipantsRequest::decode(intent.request.as_slice())
                            .map_err(|err| err.to_string())?;
                        match self.remote_rooms.list_participants(&request.room) {
                            Ok(participants) => {
                                let participants = participants
                                    .into_iter()
                                    .map(|participant| {
                                        proto::ParticipantInfo::decode(participant.encode_to_vec().as_slice())
                                            .map_err(|err| err.to_string())
                                    })
                                    .collect::<Result<Vec<_>, _>>()?;
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success {
                                    response: proto::ListParticipantsResponse { participants }
                                        .encode_to_vec(),
                                }
                            }
                            Err(err) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 404,
                                    code: "not_found".to_string(),
                                    msg: err.to_string(),
                                }
                            }
                        }
                    }
                    "UpdateSubscriptions" => {
                        let _ = proto::UpdateSubscriptionsRequest::decode(intent.request.as_slice())
                            .map_err(|err| err.to_string())?;
                        oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success {
                            response: proto::UpdateSubscriptionsResponse::default().encode_to_vec(),
                        }
                    }
                    "SendData" => {
                        let _ = proto::SendDataRequest::decode(intent.request.as_slice())
                            .map_err(|err| err.to_string())?;
                        oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success {
                            response: proto::SendDataResponse::default().encode_to_vec(),
                        }
                    }
                    "DeleteRoom" => {
                        let request = proto::DeleteRoomRequest::decode(intent.request.as_slice())
                            .map_err(|err| err.to_string())?;
                        match self.remote_rooms.delete_room(&request.room) {
                            Ok(_) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success {
                                    response: proto::DeleteRoomResponse::default().encode_to_vec(),
                                }
                            }
                            Err(err) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 404,
                                    code: "not_found".to_string(),
                                    msg: err.to_string(),
                                }
                            }
                        }
                    }
                    "RemoveParticipant" => {
                        let request = proto::RoomParticipantIdentity::decode(intent.request.as_slice())
                            .map_err(|err| err.to_string())?;
                        match self
                            .remote_rooms
                            .remove_participant(&request.room, &request.identity)
                        {
                            Ok(_) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::Success {
                                    response: proto::RemoveParticipantResponse::default()
                                        .encode_to_vec(),
                                }
                            }
                            Err(err) => {
                                oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                                    status: 404,
                                    code: "not_found".to_string(),
                                    msg: err.to_string(),
                                }
                            }
                        }
                    }
                    _ => oxidesfu_signaling::NonLocalRelayRoomServiceResponse::TwirpError {
                        status: 501,
                        code: "unimplemented".to_string(),
                        msg: format!("unsupported relay room service method: {}", intent.method),
                    },
                };

                mailbox
                    .store_room_service_response(&receipt, &response)
                    .map_err(|err| err.to_string())?;
            }

            Ok(())
        }
    }
    impl RecordingRelayDispatcher {
        fn take(&self) -> Vec<NonLocalRelayJoinIntent> {
            self.intents
                .lock()
                .expect("relay intents lock should not be poisoned")
                .clone()
        }
    }
    impl NonLocalRelayDispatcher for RecordingRelayDispatcher {
        fn dispatch_non_local_join(
            &self,
            intent: NonLocalRelayJoinIntent,
        ) -> Result<Option<oxidesfu_signaling::NonLocalRelayJoinResponse>, String> {
            self.intents
                .lock()
                .expect("relay intents lock should not be poisoned")
                .push(intent);
            Ok(None)
        }

        fn dispatch_non_local_termination(
            &self,
            _intent: oxidesfu_signaling::NonLocalRelaySessionTerminationIntent,
        ) -> Result<(), String> {
            Ok(())
        }
    }
    #[derive(Debug)]
    struct PlacementProbeDirectory {
        inner: RoomNodeRegistry,
        selected_rooms: Mutex<Vec<String>>,
        force_select_error: bool,
    }
    impl PlacementProbeDirectory {
        fn new(force_select_error: bool) -> Self {
            Self {
                inner: RoomNodeRegistry::default(),
                selected_rooms: Mutex::new(Vec::new()),
                force_select_error,
            }
        }

        fn selected_rooms(&self) -> Vec<String> {
            self.selected_rooms
                .lock()
                .expect("selected room lock should not be poisoned")
                .clone()
        }
    }
    impl RoomNodeDirectory for PlacementProbeDirectory {
        fn register_node(&self, node: RegisteredNode) -> Result<(), RoomNodeRegistryError> {
            self.inner.register_node(node)
        }

        fn unregister_node(&self, node_id: &str) -> Result<(), RoomNodeRegistryError> {
            self.inner.unregister_node(node_id)
        }

        fn list_nodes(&self) -> Result<Vec<RegisteredNode>, RoomNodeRegistryError> {
            self.inner.list_nodes()
        }

        fn get_node_for_room(
            &self,
            room_name: &str,
        ) -> Result<RegisteredNode, RoomNodeRegistryError> {
            self.inner.get_node_for_room(room_name)
        }

        fn set_node_for_room(
            &self,
            room_name: &str,
            node_id: &str,
        ) -> Result<(), RoomNodeRegistryError> {
            self.inner.set_node_for_room(room_name, node_id)
        }

        fn clear_room_state(&self, room_name: &str) -> Result<(), RoomNodeRegistryError> {
            self.inner.clear_room_state(room_name)
        }

        fn select_or_assign_node_for_room(
            &self,
            room_name: &str,
        ) -> Result<RegisteredNode, RoomNodeRegistryError> {
            self.selected_rooms
                .lock()
                .expect("selected room lock should not be poisoned")
                .push(room_name.to_string());
            if self.force_select_error {
                return Err(RoomNodeRegistryError::Backend {
                    message: "forced placement error".to_string(),
                });
            }
            self.inner.select_or_assign_node_for_room(room_name)
        }

        fn set_node_draining(
            &self,
            node_id: &str,
            draining: bool,
        ) -> Result<(), RoomNodeRegistryError> {
            self.inner.set_node_draining(node_id, draining)
        }

        fn is_node_draining(&self, node_id: &str) -> Result<bool, RoomNodeRegistryError> {
            self.inner.is_node_draining(node_id)
        }
    }
    async fn spawn_ready_redis_server_for_distributed_tests()
    -> Option<(tokio::process::Child, String)> {
        let redis_port = reserve_local_port();
        let mut redis = spawn_redis_server(redis_port).await?;

        let redis_url = format!("redis://127.0.0.1:{redis_port}/0");
        if wait_for_redis_ready(&redis_url).await.is_err() {
            let _ = redis.kill().await;
            return None;
        }

        Some((redis, redis_url))
    }
    async fn spawn_redis_server(port: u16) -> Option<tokio::process::Child> {
        let mut command = tokio::process::Command::new("redis-server");
        command.kill_on_drop(true);
        command
            .arg("--save")
            .arg("")
            .arg("--appendonly")
            .arg("no")
            .arg("--bind")
            .arg("127.0.0.1")
            .arg("--port")
            .arg(port.to_string())
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        match command.spawn() {
            Ok(child) => Some(child),
            Err(err) if err.kind() == ErrorKind::NotFound => {
                spawn_redis_server_via_docker(port).await
            }
            Err(err) => panic!("failed to spawn redis-server: {err}"),
        }
    }

    async fn spawn_redis_server_via_docker(port: u16) -> Option<tokio::process::Child> {
        let mut command = tokio::process::Command::new("docker");
        command.kill_on_drop(true);
        command
            .arg("run")
            .arg("--rm")
            .arg("--pull")
            .arg("missing")
            .arg("-p")
            .arg(format!("127.0.0.1:{port}:6379"))
            .arg("redis:7-alpine")
            .stdout(Stdio::null())
            .stderr(Stdio::null());

        match command.spawn() {
            Ok(child) => Some(child),
            Err(err) if err.kind() == ErrorKind::NotFound => None,
            Err(err) => panic!("failed to spawn dockerized redis-server: {err}"),
        }
    }
    async fn wait_for_redis_ready(redis_url: &str) -> Result<(), String> {
        let client = redis::Client::open(redis_url)
            .map_err(|err| format!("failed to create redis client: {err}"))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);

        loop {
            let ping_result = (|| -> Result<(), String> {
                let mut conn = client
                    .get_connection()
                    .map_err(|err| format!("failed to open redis connection: {err}"))?;
                let pong = redis::cmd("PING")
                    .query::<String>(&mut conn)
                    .map_err(|err| format!("redis PING failed: {err}"))?;
                if pong == "PONG" {
                    Ok(())
                } else {
                    Err(format!("unexpected redis ping response: {pong}"))
                }
            })();

            match ping_result {
                Ok(()) => return Ok(()),
                Err(err) => {
                    if tokio::time::Instant::now() >= deadline {
                        return Err(format!(
                            "redis did not become ready at {redis_url} within {:?}: {err}",
                            Duration::from_secs(8)
                        ));
                    }
                    tokio::time::sleep(Duration::from_millis(75)).await;
                }
            }
        }
    }
    async fn wait_for_room_node_registration_count(
        redis_url: &str,
        expected_count: usize,
    ) -> Result<(), String> {
        let store = oxidesfu_room::RedisHashClient::from_url(redis_url)
            .map_err(|err| format!("failed to construct redis hash store: {err}"))?;
        let deadline = tokio::time::Instant::now() + Duration::from_secs(8);

        loop {
            let values = RedisHashStore::hvals(&store, "oxidesfu:nodes")
                .map_err(|err| format!("failed to list redis node registry: {err}"))?;
            if values.len() >= expected_count {
                return Ok(());
            }
            if tokio::time::Instant::now() >= deadline {
                return Err(format!(
                    "expected at least {expected_count} registered nodes but observed {}",
                    values.len()
                ));
            }
            tokio::time::sleep(Duration::from_millis(75)).await;
        }
    }
    fn force_room_assignment_to_node(
        redis_url: &str,
        room_name: &str,
        node_id: &str,
    ) -> Result<(), String> {
        let store = oxidesfu_room::RedisHashClient::from_url(redis_url)
            .map_err(|err| format!("failed to construct redis hash store: {err}"))?;
        RedisHashStore::hset(&store, "oxidesfu:room_node_map", room_name, node_id)
            .map_err(|err| format!("failed to assign room to node in redis: {err}"))
    }
