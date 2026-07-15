use std::{collections::HashMap, thread, time::Duration};

use livekit_protocol as proto;

use crate::{
    RoomInternalCompat, RoomStore, RoomStoreError,
    store::{RoomRecord, StoredAgentDispatch},
};

const MAX_AGENT_DISPATCH_METADATA_BYTES: usize = 512 * 1024;
const MAX_AGENT_DISPATCH_ATTRIBUTES_BYTES: usize = 64 * 1024;

fn room_snapshot(record: &RoomRecord) -> proto::Room {
    let mut room = record.room.clone();
    room.num_publishers = record
        .participants
        .values()
        .filter(|participant| !participant.tracks.is_empty())
        .count()
        .min(u32::MAX as usize) as u32;
    room
}

fn default_enabled_codecs() -> Vec<proto::Codec> {
    vec![
        proto::Codec {
            mime: "audio/opus".to_string(),
            ..Default::default()
        },
        proto::Codec {
            mime: "video/vp8".to_string(),
            ..Default::default()
        },
    ]
}

impl RoomStore {
    /// Creates a room from a LiveKit-compatible create request.
    pub fn create_room(
        &self,
        request: proto::CreateRoomRequest,
    ) -> Result<proto::Room, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        if let Some(record) = inner.rooms.get(&request.name) {
            return Ok(record.room.clone());
        }

        let room_name = request.name;
        let room_metadata = request.metadata;
        let room_empty_timeout = request.empty_timeout;
        let room_departure_timeout = request.departure_timeout;
        let room_max_participants = request.max_participants;
        let room_agent_dispatches = request.agents;

        inner.next_room_id = inner.next_room_id.saturating_add(1);
        let now_ms = crate::store::now_unix_ms();
        let room = proto::Room {
            sid: format!("RM_{:016x}", inner.next_room_id),
            name: room_name,
            empty_timeout: if room_empty_timeout == 0 {
                300
            } else {
                room_empty_timeout
            },
            departure_timeout: if room_departure_timeout == 0 {
                20
            } else {
                room_departure_timeout
            },
            max_participants: room_max_participants,
            creation_time: now_ms / 1000,
            creation_time_ms: now_ms,
            metadata: room_metadata,
            enabled_codecs: default_enabled_codecs(),
            ..Default::default()
        };
        let mut stored_agent_dispatches = Vec::with_capacity(room_agent_dispatches.len());
        for dispatch in room_agent_dispatches {
            inner.next_agent_dispatch_id = inner.next_agent_dispatch_id.saturating_add(1);
            let agent_dispatch = proto::AgentDispatch {
                id: format!("AD_{:016x}", inner.next_agent_dispatch_id),
                agent_name: dispatch.agent_name,
                room: room.name.clone(),
                metadata: dispatch.metadata,
                state: Some(proto::AgentDispatchState {
                    created_at: now_ms,
                    ..Default::default()
                }),
                restart_policy: dispatch.restart_policy,
                deployment: dispatch.deployment,
                attributes: dispatch.attributes,
            };
            stored_agent_dispatches.push(StoredAgentDispatch {
                dispatch: agent_dispatch,
            });
        }

        inner.rooms.insert(
            room.name.clone(),
            RoomRecord {
                room: room.clone(),
                room_internal: None,
                participants: HashMap::new(),
                participant_versions: HashMap::new(),
                agent_dispatches: stored_agent_dispatches,
                empty_since_unix_ms: Some(now_ms),
                had_participants: false,
            },
        );
        Ok(room)
    }

    /// Lists all rooms, or only rooms matching `names` when non-empty.
    pub fn list_rooms(&self, names: &[String]) -> Result<Vec<proto::Room>, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        if names.is_empty() {
            let mut rooms = inner.rooms.values().map(room_snapshot).collect::<Vec<_>>();
            rooms.sort_by(|a, b| a.name.cmp(&b.name));
            return Ok(rooms);
        }

        Ok(names
            .iter()
            .filter_map(|name| inner.rooms.get(name).map(room_snapshot))
            .collect())
    }

    /// Deletes a room by name.
    pub fn delete_room(&self, room: &str) -> Result<(), RoomStoreError> {
        self.delete_room_with_snapshot(room).map(|_| ())
    }

    /// Deletes a room by name and returns the deleted room snapshot.
    pub fn delete_room_with_snapshot(&self, room: &str) -> Result<proto::Room, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        match inner.rooms.remove(room) {
            Some(record) => {
                inner.room_locks.remove(room);
                let unsubscribed_before = inner.media_unsubscribed.len();
                inner.media_unsubscribed.retain(
                    |(candidate_room, _publisher, _track_sid, _subscriber)| candidate_room != room,
                );
                if inner.media_unsubscribed.len() != unsubscribed_before {
                    inner.media_subscription_revision =
                        inner.media_subscription_revision.saturating_add(1);
                }
                Ok(record.room)
            }
            None => Err(RoomStoreError::RoomNotFound),
        }
    }

    /// Acquires a per-room lock token, waiting until the lock becomes available.
    pub fn lock_room(&self, room: &str, lock_interval: Duration) -> Result<String, RoomStoreError> {
        if room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "room name cannot be empty".to_string(),
            ));
        }

        let lock_interval_ms = lock_interval.as_millis().min(i64::MAX as u128) as i64;
        if lock_interval_ms <= 0 {
            return Err(RoomStoreError::InvalidArgument(
                "lock interval must be greater than zero".to_string(),
            ));
        }

        loop {
            let now_ms = crate::store::now_unix_ms();
            if let Ok(mut inner) = self.inner.write() {
                let can_acquire = match inner.room_locks.get(room) {
                    Some(lock) => lock.expires_at_unix_ms <= now_ms,
                    None => true,
                };

                if can_acquire {
                    inner.next_room_lock_token_id = inner.next_room_lock_token_id.saturating_add(1);
                    let token = format!("rl_{:016x}", inner.next_room_lock_token_id);
                    inner.room_locks.insert(
                        room.to_string(),
                        crate::store::RoomLockState {
                            token: token.clone(),
                            expires_at_unix_ms: now_ms.saturating_add(lock_interval_ms),
                        },
                    );
                    return Ok(token);
                }
            } else {
                return Err(RoomStoreError::LockPoisoned);
            }

            thread::sleep(Duration::from_millis(1));
        }
    }

    /// Releases a room lock token if it matches the currently held token.
    pub fn unlock_room(&self, room: &str, token: &str) -> Result<(), RoomStoreError> {
        if room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "room name cannot be empty".to_string(),
            ));
        }
        if token.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "lock token cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        if inner
            .room_locks
            .get(room)
            .is_some_and(|lock| lock.token == token)
        {
            inner.room_locks.remove(room);
        }

        Ok(())
    }

    /// Returns configured room agent dispatch entries for `room`.
    pub fn room_agent_dispatches(
        &self,
        room: &str,
    ) -> Result<Vec<proto::RoomAgentDispatch>, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner.rooms.get(room).ok_or(RoomStoreError::RoomNotFound)?;
        Ok(record
            .agent_dispatches
            .iter()
            .map(|stored| proto::RoomAgentDispatch {
                agent_name: stored.dispatch.agent_name.clone(),
                metadata: stored.dispatch.metadata.clone(),
                restart_policy: stored.dispatch.restart_policy,
                deployment: stored.dispatch.deployment.clone(),
                attributes: stored.dispatch.attributes.clone(),
            })
            .collect())
    }

    pub fn create_agent_dispatch(
        &self,
        request: proto::CreateAgentDispatchRequest,
    ) -> Result<proto::AgentDispatch, RoomStoreError> {
        if request.room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "room name cannot be empty".to_string(),
            ));
        }
        if request.metadata.len() > MAX_AGENT_DISPATCH_METADATA_BYTES {
            return Err(RoomStoreError::InvalidArgument(
                "metadata exceeds 512KiB limit".to_string(),
            ));
        }
        let attributes_size: usize = request
            .attributes
            .iter()
            .map(|(key, value)| key.len() + value.len())
            .sum();
        if attributes_size > MAX_AGENT_DISPATCH_ATTRIBUTES_BYTES {
            return Err(RoomStoreError::InvalidArgument(
                "attributes exceed 64KiB limit".to_string(),
            ));
        }

        let room = request.room;
        let agent_name = request.agent_name;
        let metadata = request.metadata;
        let restart_policy = request.restart_policy;
        let deployment = request.deployment;
        let attributes = request.attributes;

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner.next_agent_dispatch_id = inner.next_agent_dispatch_id.saturating_add(1);
        let dispatch_id = format!("AD_{:016x}", inner.next_agent_dispatch_id);
        let created_at = crate::store::now_unix_ms();

        let dispatch = proto::AgentDispatch {
            id: dispatch_id,
            agent_name,
            room: room.clone(),
            metadata,
            state: Some(proto::AgentDispatchState {
                created_at,
                ..Default::default()
            }),
            restart_policy,
            deployment,
            attributes,
        };

        let record = inner
            .rooms
            .get_mut(&room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        record.agent_dispatches.push(StoredAgentDispatch {
            dispatch: dispatch.clone(),
        });
        Ok(dispatch)
    }

    pub fn list_agent_dispatches(
        &self,
        room: &str,
        dispatch_id: &str,
    ) -> Result<Vec<proto::AgentDispatch>, RoomStoreError> {
        if room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "room name cannot be empty".to_string(),
            ));
        }
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner.rooms.get(room).ok_or(RoomStoreError::RoomNotFound)?;

        let jobs_by_id = inner.stored_agent_jobs_by_room.get(room);
        let mut dispatches = record
            .agent_dispatches
            .iter()
            .map(|stored| {
                let mut dispatch = stored.dispatch.clone();
                if let Some(jobs_by_id) = jobs_by_id {
                    let mut jobs = jobs_by_id
                        .values()
                        .filter(|job| job.dispatch_id == dispatch.id)
                        .cloned()
                        .collect::<Vec<_>>();
                    if !jobs.is_empty() {
                        let state = dispatch
                            .state
                            .get_or_insert_with(proto::AgentDispatchState::default);
                        state.jobs.append(&mut jobs);
                    }
                }
                dispatch
            })
            .collect::<Vec<_>>();
        if !dispatch_id.is_empty() {
            dispatches.retain(|dispatch| dispatch.id == dispatch_id);
        }
        Ok(dispatches)
    }

    pub fn delete_agent_dispatch(
        &self,
        room: &str,
        dispatch_id: &str,
    ) -> Result<proto::AgentDispatch, RoomStoreError> {
        if room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "room name cannot be empty".to_string(),
            ));
        }
        if dispatch_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "dispatch id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;

        let Some(index) = record
            .agent_dispatches
            .iter()
            .position(|stored| stored.dispatch.id == dispatch_id)
        else {
            return Err(RoomStoreError::AgentDispatchNotFound);
        };

        let mut deleted = record.agent_dispatches.remove(index).dispatch;
        let state = deleted
            .state
            .get_or_insert_with(proto::AgentDispatchState::default);
        state.deleted_at = crate::store::now_unix_ms();
        Ok(deleted)
    }

    /// Stores an agent dispatch record in the compatibility store path.
    pub fn store_agent_dispatch_record(
        &self,
        dispatch: &proto::AgentDispatch,
    ) -> Result<(), RoomStoreError> {
        if dispatch.room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "dispatch room cannot be empty".to_string(),
            ));
        }
        if dispatch.id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "dispatch id cannot be empty".to_string(),
            ));
        }

        let mut clone = dispatch.clone();
        if let Some(state) = clone.state.as_mut() {
            state.jobs.clear();
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .stored_agent_dispatches_by_room
            .entry(clone.room.clone())
            .or_default()
            .insert(clone.id.clone(), clone);
        Ok(())
    }

    /// Deletes an agent dispatch record in the compatibility store path.
    pub fn delete_agent_dispatch_record(
        &self,
        dispatch: &proto::AgentDispatch,
    ) -> Result<(), RoomStoreError> {
        if dispatch.room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "dispatch room cannot be empty".to_string(),
            ));
        }
        if dispatch.id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "dispatch id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        if let Some(by_id) = inner
            .stored_agent_dispatches_by_room
            .get_mut(&dispatch.room)
        {
            by_id.remove(&dispatch.id);
            if by_id.is_empty() {
                inner.stored_agent_dispatches_by_room.remove(&dispatch.room);
            }
        }
        Ok(())
    }

    /// Lists agent dispatches for a room in the compatibility store path.
    pub fn list_agent_dispatch_records(
        &self,
        room: &str,
    ) -> Result<Vec<proto::AgentDispatch>, RoomStoreError> {
        if room.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "room name cannot be empty".to_string(),
            ));
        }

        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut dispatches = inner
            .stored_agent_dispatches_by_room
            .get(room)
            .map(|by_id| by_id.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();

        let jobs = inner
            .stored_agent_jobs_by_room
            .get(room)
            .map(|by_id| by_id.values().cloned().collect::<Vec<_>>())
            .unwrap_or_default();

        let mut index = std::collections::HashMap::new();
        for (i, dispatch) in dispatches.iter().enumerate() {
            index.insert(dispatch.id.clone(), i);
        }

        for job in jobs {
            if let Some(i) = index.get(&job.dispatch_id).copied() {
                let state = dispatches[i]
                    .state
                    .get_or_insert_with(proto::AgentDispatchState::default);
                state.jobs.push(job);
            }
        }

        Ok(dispatches)
    }

    /// Stores an agent job record in the compatibility store path.
    pub fn store_agent_job_record(&self, job: &proto::Job) -> Result<(), RoomStoreError> {
        let room_name = job
            .room
            .as_ref()
            .map(|room| room.name.trim())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                RoomStoreError::InvalidArgument("job room cannot be empty".to_string())
            })?;
        if job.id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "job id cannot be empty".to_string(),
            ));
        }

        let mut clone = job.clone();
        clone.room = None;
        if let Some(participant) = clone.participant.as_ref() {
            clone.participant = Some(proto::ParticipantInfo {
                identity: participant.identity.clone(),
                ..Default::default()
            });
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .stored_agent_jobs_by_room
            .entry(room_name.to_string())
            .or_default()
            .insert(clone.id.clone(), clone);

        Ok(())
    }

    /// Deletes an agent job record in the compatibility store path.
    pub fn delete_agent_job_record(&self, job: &proto::Job) -> Result<(), RoomStoreError> {
        let room_name = job
            .room
            .as_ref()
            .map(|room| room.name.trim())
            .filter(|name| !name.is_empty())
            .ok_or_else(|| {
                RoomStoreError::InvalidArgument("job room cannot be empty".to_string())
            })?;
        if job.id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "job id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        if let Some(by_id) = inner.stored_agent_jobs_by_room.get_mut(room_name) {
            by_id.remove(&job.id);
            if by_id.is_empty() {
                inner.stored_agent_jobs_by_room.remove(room_name);
            }
        }
        Ok(())
    }

    /// Stores an egress info record in the compatibility store path.
    pub fn store_egress_info(&self, info: &proto::EgressInfo) -> Result<(), RoomStoreError> {
        if info.egress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "egress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .egress_infos
            .insert(info.egress_id.clone(), info.clone());
        Ok(())
    }

    /// Loads an egress info record by ID.
    pub fn load_egress_info(&self, egress_id: &str) -> Result<proto::EgressInfo, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .egress_infos
            .get(egress_id)
            .cloned()
            .ok_or(RoomStoreError::EgressNotFound)
    }

    /// Updates an egress info record by replacing stored value for the ID.
    pub fn update_egress_info(&self, info: &proto::EgressInfo) -> Result<(), RoomStoreError> {
        self.store_egress_info(info)
    }

    fn next_egress_id(inner: &mut crate::store::RoomStoreInner) -> String {
        inner.next_egress_id = inner.next_egress_id.saturating_add(1);
        format!("EG_{:016x}", inner.next_egress_id)
    }

    fn stream_results_from_outputs(outputs: &[proto::Output], now: i64) -> Vec<proto::StreamInfo> {
        let mut stream_results = Vec::new();
        for output in outputs {
            if let Some(proto::output::Config::Stream(stream)) = output.config.as_ref() {
                for url in &stream.urls {
                    let trimmed = url.trim();
                    if trimmed.is_empty() {
                        continue;
                    }
                    stream_results.push(proto::StreamInfo {
                        url: trimmed.to_string(),
                        started_at: now,
                        status: proto::stream_info::Status::Active as i32,
                        ..Default::default()
                    });
                }
            }
        }
        stream_results
    }

    fn stream_results_from_v1_stream_outputs(
        outputs: &[proto::StreamOutput],
        now: i64,
    ) -> Vec<proto::StreamInfo> {
        let mut stream_results = Vec::new();
        for output in outputs {
            for url in &output.urls {
                let trimmed = url.trim();
                if trimmed.is_empty() {
                    continue;
                }
                stream_results.push(proto::StreamInfo {
                    url: trimmed.to_string(),
                    started_at: now,
                    status: proto::stream_info::Status::Active as i32,
                    ..Default::default()
                });
            }
        }
        stream_results
    }

    fn store_started_egress_info(
        &self,
        room_name: String,
        source_type: proto::EgressSourceType,
        request: proto::egress_info::Request,
        stream_results: Vec<proto::StreamInfo>,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        let now = crate::store::now_unix_ms();
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let egress_id = Self::next_egress_id(&mut inner);

        let info = proto::EgressInfo {
            egress_id: egress_id.clone(),
            room_name,
            source_type: source_type as i32,
            status: proto::EgressStatus::EgressStarting as i32,
            started_at: now,
            updated_at: now,
            stream_results,
            request: Some(request),
            ..Default::default()
        };

        inner.egress_infos.insert(egress_id, info.clone());
        Ok(info)
    }

    /// Starts an in-memory egress record from a `StartEgressRequest`.
    pub fn start_egress_info(
        &self,
        request: &proto::StartEgressRequest,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        if request.outputs.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "at least one egress output is required".to_string(),
            ));
        }

        let source_type = match request.source {
            Some(proto::start_egress_request::Source::Web(_)) => proto::EgressSourceType::Web,
            _ => proto::EgressSourceType::Sdk,
        };

        self.store_started_egress_info(
            request.room_name.clone(),
            source_type,
            proto::egress_info::Request::Egress(request.clone()),
            Self::stream_results_from_outputs(&request.outputs, crate::store::now_unix_ms()),
        )
    }

    /// Starts an in-memory room-composite egress record.
    pub fn start_room_composite_egress_info(
        &self,
        request: &proto::RoomCompositeEgressRequest,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        self.store_started_egress_info(
            request.room_name.clone(),
            proto::EgressSourceType::Web,
            proto::egress_info::Request::RoomComposite(request.clone()),
            Self::stream_results_from_v1_stream_outputs(
                &request.stream_outputs,
                crate::store::now_unix_ms(),
            ),
        )
    }

    /// Starts an in-memory web egress record.
    pub fn start_web_egress_info(
        &self,
        request: &proto::WebEgressRequest,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        self.store_started_egress_info(
            String::new(),
            proto::EgressSourceType::Web,
            proto::egress_info::Request::Web(request.clone()),
            Self::stream_results_from_v1_stream_outputs(
                &request.stream_outputs,
                crate::store::now_unix_ms(),
            ),
        )
    }

    /// Starts an in-memory participant egress record.
    pub fn start_participant_egress_info(
        &self,
        request: &proto::ParticipantEgressRequest,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        self.store_started_egress_info(
            request.room_name.clone(),
            proto::EgressSourceType::Sdk,
            proto::egress_info::Request::Participant(request.clone()),
            Self::stream_results_from_v1_stream_outputs(
                &request.stream_outputs,
                crate::store::now_unix_ms(),
            ),
        )
    }

    /// Starts an in-memory track-composite egress record.
    pub fn start_track_composite_egress_info(
        &self,
        request: &proto::TrackCompositeEgressRequest,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        self.store_started_egress_info(
            request.room_name.clone(),
            proto::EgressSourceType::Sdk,
            proto::egress_info::Request::TrackComposite(request.clone()),
            Self::stream_results_from_v1_stream_outputs(
                &request.stream_outputs,
                crate::store::now_unix_ms(),
            ),
        )
    }

    /// Starts an in-memory track egress record.
    pub fn start_track_egress_info(
        &self,
        request: &proto::TrackEgressRequest,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        self.store_started_egress_info(
            request.room_name.clone(),
            proto::EgressSourceType::Sdk,
            proto::egress_info::Request::Track(request.clone()),
            Vec::new(),
        )
    }

    fn assert_egress_can_be_updated(info: &proto::EgressInfo) -> Result<(), RoomStoreError> {
        let status = proto::EgressStatus::try_from(info.status)
            .unwrap_or(proto::EgressStatus::EgressStarting);
        match status {
            proto::EgressStatus::EgressStarting | proto::EgressStatus::EgressActive => Ok(()),
            proto::EgressStatus::EgressEnding
            | proto::EgressStatus::EgressComplete
            | proto::EgressStatus::EgressFailed
            | proto::EgressStatus::EgressAborted
            | proto::EgressStatus::EgressLimitReached => {
                Err(RoomStoreError::InvalidArgument(format!(
                    "egress with status {} cannot be updated",
                    status.as_str_name()
                )))
            }
        }
    }

    /// Updates the layout metadata of an egress record.
    pub fn update_egress_layout(
        &self,
        egress_id: &str,
        layout: &str,
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        if egress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "egress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let info = inner
            .egress_infos
            .get_mut(egress_id)
            .ok_or(RoomStoreError::EgressNotFound)?;
        Self::assert_egress_can_be_updated(info)?;

        if let Some(request) = info.request.as_mut() {
            match request {
                proto::egress_info::Request::RoomComposite(request) => {
                    request.layout = layout.to_string();
                }
                proto::egress_info::Request::Egress(request) => {
                    if let Some(proto::start_egress_request::Source::Template(template)) =
                        request.source.as_mut()
                    {
                        template.layout = layout.to_string();
                    }
                }
                _ => {}
            }
        }

        info.updated_at = crate::store::now_unix_ms();
        Ok(info.clone())
    }

    /// Updates stream output urls of an egress record.
    pub fn update_egress_stream_urls(
        &self,
        egress_id: &str,
        add_output_urls: &[String],
        remove_output_urls: &[String],
    ) -> Result<proto::EgressInfo, RoomStoreError> {
        if egress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "egress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let info = inner
            .egress_infos
            .get_mut(egress_id)
            .ok_or(RoomStoreError::EgressNotFound)?;
        Self::assert_egress_can_be_updated(info)?;

        info.stream_results
            .retain(|stream| !remove_output_urls.iter().any(|url| url == &stream.url));

        for url in add_output_urls {
            let trimmed = url.trim();
            if trimmed.is_empty() {
                continue;
            }
            if info
                .stream_results
                .iter()
                .any(|stream| stream.url == trimmed)
            {
                continue;
            }
            info.stream_results.push(proto::StreamInfo {
                url: trimmed.to_string(),
                started_at: crate::store::now_unix_ms(),
                status: proto::stream_info::Status::Active as i32,
                ..Default::default()
            });
        }

        info.updated_at = crate::store::now_unix_ms();
        Ok(info.clone())
    }

    /// Stops an egress info record by marking it complete and setting `ended_at`.
    pub fn stop_egress_info(&self, egress_id: &str) -> Result<proto::EgressInfo, RoomStoreError> {
        if egress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "egress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let info = inner
            .egress_infos
            .get_mut(egress_id)
            .ok_or(RoomStoreError::EgressNotFound)?;

        let status = proto::EgressStatus::try_from(info.status)
            .unwrap_or(proto::EgressStatus::EgressStarting);
        match status {
            proto::EgressStatus::EgressComplete
            | proto::EgressStatus::EgressFailed
            | proto::EgressStatus::EgressAborted
            | proto::EgressStatus::EgressLimitReached => {
                return Err(RoomStoreError::InvalidArgument(format!(
                    "egress with status {} cannot be stopped",
                    status.as_str_name()
                )));
            }
            proto::EgressStatus::EgressStarting
            | proto::EgressStatus::EgressActive
            | proto::EgressStatus::EgressEnding => {}
        }

        info.status = proto::EgressStatus::EgressComplete as i32;
        if info.ended_at == 0 {
            info.ended_at = crate::store::now_unix_ms();
        }

        Ok(info.clone())
    }

    /// Lists egress info records, optionally filtered by room name.
    pub fn list_egress_infos(
        &self,
        room_name: &str,
        active_only: bool,
    ) -> Result<Vec<proto::EgressInfo>, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut infos = inner
            .egress_infos
            .values()
            .filter(|info| room_name.is_empty() || info.room_name == room_name)
            .filter(|info| {
                if !active_only {
                    return true;
                }
                info.ended_at == 0
            })
            .cloned()
            .collect::<Vec<_>>();

        infos.sort_by(|a, b| a.egress_id.cmp(&b.egress_id));
        Ok(infos)
    }

    /// Removes ended egress records from the compatibility store path.
    pub fn clean_ended_egress_infos(&self) -> Result<(), RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner.egress_infos.retain(|_, info| info.ended_at == 0);
        Ok(())
    }

    /// Stores ingress info in the compatibility store path.
    pub fn store_ingress_info(&self, info: &proto::IngressInfo) -> Result<(), RoomStoreError> {
        if info.ingress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "ingress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .ingress_infos
            .insert(info.ingress_id.clone(), info.clone());
        Ok(())
    }

    /// Updates ingress info by replacing stored value for the ID.
    pub fn update_ingress_info(&self, info: &proto::IngressInfo) -> Result<(), RoomStoreError> {
        self.store_ingress_info(info)
    }

    /// Updates ingress state with out-of-date guard on `started_at`.
    pub fn update_ingress_state(
        &self,
        ingress_id: &str,
        state: &proto::IngressState,
    ) -> Result<(), RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let info = inner
            .ingress_infos
            .get_mut(ingress_id)
            .ok_or(RoomStoreError::IngressNotFound)?;

        let existing_started_at = info
            .state
            .as_ref()
            .map(|existing| existing.started_at)
            .unwrap_or(0);
        if state.started_at < existing_started_at {
            return Err(RoomStoreError::InvalidArgument(
                "ingress state out of date".to_string(),
            ));
        }

        info.state = Some(state.clone());
        Ok(())
    }

    /// Loads ingress info by ID.
    pub fn load_ingress_info(
        &self,
        ingress_id: &str,
    ) -> Result<proto::IngressInfo, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .ingress_infos
            .get(ingress_id)
            .cloned()
            .ok_or(RoomStoreError::IngressNotFound)
    }

    /// Lists ingress info, optionally filtered by room name.
    pub fn list_ingress_infos(
        &self,
        room_name: &str,
    ) -> Result<Vec<proto::IngressInfo>, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut infos = inner
            .ingress_infos
            .values()
            .filter(|info| room_name.is_empty() || info.room_name == room_name)
            .cloned()
            .collect::<Vec<_>>();
        infos.sort_by(|a, b| a.ingress_id.cmp(&b.ingress_id));
        Ok(infos)
    }

    /// Deletes ingress info by ID.
    pub fn delete_ingress_info(&self, info: &proto::IngressInfo) -> Result<(), RoomStoreError> {
        if info.ingress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "ingress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner.ingress_infos.remove(&info.ingress_id);
        Ok(())
    }

    fn validate_and_normalize_url_input_url(raw_url: &str) -> Result<String, RoomStoreError> {
        let normalized = raw_url.trim();
        if normalized.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "missing URL parameter".to_string(),
            ));
        }

        let Some(scheme_end) = normalized.find("://") else {
            return Err(RoomStoreError::InvalidArgument(
                "invalid url format".to_string(),
            ));
        };

        let scheme = normalized[..scheme_end].to_ascii_lowercase();
        if !matches!(scheme.as_str(), "http" | "https" | "srt") {
            return Err(RoomStoreError::InvalidArgument(format!(
                "invalid url scheme {scheme}"
            )));
        }

        Ok(normalized.to_string())
    }

    /// Creates ingress info from a `CreateIngressRequest` and stores it.
    #[allow(deprecated)] // Preserve legacy `bypass_transcoding` protocol compatibility.
    pub fn create_ingress_info(
        &self,
        request: &proto::CreateIngressRequest,
    ) -> Result<proto::IngressInfo, RoomStoreError> {
        let input_type = proto::IngressInput::try_from(request.input_type).map_err(|_| {
            RoomStoreError::InvalidArgument("invalid ingress input type".to_string())
        })?;

        let normalized_url = if matches!(input_type, proto::IngressInput::UrlInput) {
            Self::validate_and_normalize_url_input_url(&request.url)?
        } else {
            request.url.trim().to_string()
        };

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner.next_ingress_id = inner.next_ingress_id.saturating_add(1);
        let ingress_id = format!("IN_{:016x}", inner.next_ingress_id);

        let stream_key = if matches!(input_type, proto::IngressInput::UrlInput) {
            String::new()
        } else {
            format!("SK_{:016x}", inner.next_ingress_id)
        };

        let reusable = matches!(
            input_type,
            proto::IngressInput::RtmpInput | proto::IngressInput::WhipInput
        );

        let mut info = proto::IngressInfo {
            ingress_id: ingress_id.clone(),
            name: request.name.clone(),
            stream_key,
            url: normalized_url,
            input_type: request.input_type,
            audio: request.audio.clone(),
            video: request.video.clone(),
            room_name: request.room_name.clone(),
            participant_identity: request.participant_identity.clone(),
            participant_name: request.participant_name.clone(),
            participant_metadata: request.participant_metadata.clone(),
            bypass_transcoding: request.bypass_transcoding,
            enable_transcoding: request.enable_transcoding,
            reusable,
            enabled: request.enabled,
            state: Some(proto::IngressState::default()),
        };

        if let Some(enable_transcoding) = info.enable_transcoding {
            info.bypass_transcoding = !enable_transcoding;
        } else if matches!(input_type, proto::IngressInput::WhipInput) {
            info.enable_transcoding = Some(false);
            info.bypass_transcoding = true;
        } else {
            info.enable_transcoding = Some(true);
        }

        inner.ingress_infos.insert(ingress_id, info.clone());
        Ok(info)
    }

    /// Updates ingress info from a `UpdateIngressRequest` and stores it.
    #[allow(deprecated)] // Preserve legacy `bypass_transcoding` protocol compatibility.
    pub fn update_ingress_from_request(
        &self,
        request: &proto::UpdateIngressRequest,
    ) -> Result<proto::IngressInfo, RoomStoreError> {
        if request.ingress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "ingress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let info = inner
            .ingress_infos
            .get_mut(&request.ingress_id)
            .ok_or(RoomStoreError::IngressNotFound)?;

        if !info.reusable {
            return Err(RoomStoreError::InvalidArgument(
                "ingress is not reusable and cannot be modified".to_string(),
            ));
        }

        let status = info
            .state
            .as_ref()
            .and_then(|state| proto::ingress_state::Status::try_from(state.status).ok())
            .unwrap_or(proto::ingress_state::Status::EndpointInactive);

        let should_apply_update = match status {
            proto::ingress_state::Status::EndpointError => {
                let state = info.state.get_or_insert_with(Default::default);
                state.status = proto::ingress_state::Status::EndpointInactive as i32;
                true
            }
            proto::ingress_state::Status::EndpointInactive
            | proto::ingress_state::Status::EndpointBuffering
            | proto::ingress_state::Status::EndpointPublishing => true,
            proto::ingress_state::Status::EndpointComplete => false,
        };

        if !should_apply_update {
            return Ok(info.clone());
        }

        if !request.name.is_empty() {
            info.name = request.name.clone();
        }
        if !request.room_name.is_empty() {
            info.room_name = request.room_name.clone();
        }
        if !request.participant_identity.is_empty() {
            info.participant_identity = request.participant_identity.clone();
        }
        if !request.participant_name.is_empty() {
            info.participant_name = request.participant_name.clone();
        }
        if !request.participant_metadata.is_empty() {
            info.participant_metadata = request.participant_metadata.clone();
        }
        if let Some(enable_transcoding) = request.enable_transcoding {
            info.enable_transcoding = Some(enable_transcoding);
            info.bypass_transcoding = !enable_transcoding;
        }
        if let Some(bypass_transcoding) = request.bypass_transcoding {
            info.bypass_transcoding = bypass_transcoding;
            info.enable_transcoding = Some(!bypass_transcoding);
        }
        if let Some(audio) = request.audio.clone() {
            info.audio = Some(audio);
        }
        if let Some(video) = request.video.clone() {
            info.video = Some(video);
        }
        if let Some(enabled) = request.enabled {
            info.enabled = Some(enabled);
        }

        Ok(info.clone())
    }

    /// Deletes ingress info by id and returns the removed record.
    pub fn delete_ingress_by_id(
        &self,
        ingress_id: &str,
    ) -> Result<proto::IngressInfo, RoomStoreError> {
        if ingress_id.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "ingress id cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let mut info = inner
            .ingress_infos
            .remove(ingress_id)
            .ok_or(RoomStoreError::IngressNotFound)?;

        info.state = Some(proto::IngressState {
            status: proto::ingress_state::Status::EndpointInactive as i32,
            ..info.state.unwrap_or_default()
        });

        Ok(info)
    }

    /// Stores a room snapshot and optional internal room metadata.
    pub fn store_room_with_internal(
        &self,
        room: &proto::Room,
        internal: Option<RoomInternalCompat>,
    ) -> Result<(), RoomStoreError> {
        if room.name.is_empty() {
            return Err(RoomStoreError::InvalidArgument(
                "room name cannot be empty".to_string(),
            ));
        }

        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let now_ms = crate::store::now_unix_ms();

        if let Some(record) = inner.rooms.get_mut(&room.name) {
            record.room = room.clone();
            record.room_internal = internal;
            return Ok(());
        }

        inner.rooms.insert(
            room.name.clone(),
            RoomRecord {
                room: room.clone(),
                room_internal: internal,
                participants: HashMap::new(),
                participant_versions: HashMap::new(),
                agent_dispatches: Vec::new(),
                empty_since_unix_ms: Some(now_ms),
                had_participants: false,
            },
        );

        Ok(())
    }

    /// Loads a room snapshot and optional internal metadata by room name.
    pub fn load_room_with_internal(
        &self,
        room_name: &str,
    ) -> Result<(proto::Room, Option<RoomInternalCompat>), RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get(room_name)
            .ok_or(RoomStoreError::RoomNotFound)?;
        Ok((record.room.clone(), record.room_internal.clone()))
    }

    /// Updates room metadata and returns the updated room.
    pub fn update_room_metadata(
        &self,
        room: &str,
        metadata: String,
    ) -> Result<proto::Room, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        let record = inner
            .rooms
            .get_mut(room)
            .ok_or(RoomStoreError::RoomNotFound)?;
        record.room.metadata = metadata;
        Ok(record.room.clone())
    }

    /// Removes rooms that have stayed empty for longer than `max_empty_age`.
    pub fn cleanup_empty_rooms_older_than(
        &self,
        max_empty_age: Duration,
    ) -> Result<usize, RoomStoreError> {
        let max_empty_age_ms = max_empty_age.as_millis().min(i64::MAX as u128) as i64;
        self.cleanup_empty_rooms_older_than_at_ms(max_empty_age_ms, crate::store::now_unix_ms())
    }

    pub(crate) fn cleanup_empty_rooms_older_than_at_ms(
        &self,
        max_empty_age_ms: i64,
        now_ms: i64,
    ) -> Result<usize, RoomStoreError> {
        self.cleanup_expired_empty_rooms_with_default_at_ms(max_empty_age_ms, now_ms)
    }

    /// Removes rooms that stayed empty past per-room timeout, falling back to `default_empty_age_ms` when unset.
    pub fn cleanup_expired_empty_rooms_with_default(
        &self,
        default_empty_age: Duration,
    ) -> Result<usize, RoomStoreError> {
        let default_empty_age_ms = default_empty_age.as_millis().min(i64::MAX as u128) as i64;
        self.cleanup_expired_empty_rooms_with_default_at_ms(
            default_empty_age_ms,
            crate::store::now_unix_ms(),
        )
    }

    pub(crate) fn cleanup_expired_empty_rooms_with_default_at_ms(
        &self,
        default_empty_age_ms: i64,
        now_ms: i64,
    ) -> Result<usize, RoomStoreError> {
        Ok(self
            .cleanup_expired_empty_rooms_with_default_at_ms_and_collect(
                default_empty_age_ms,
                now_ms,
            )?
            .len())
    }

    pub fn cleanup_expired_empty_rooms_with_default_and_collect(
        &self,
        default_empty_age: Duration,
    ) -> Result<Vec<proto::Room>, RoomStoreError> {
        let default_empty_age_ms = default_empty_age.as_millis().min(i64::MAX as u128) as i64;
        self.cleanup_expired_empty_rooms_with_default_at_ms_and_collect(
            default_empty_age_ms,
            crate::store::now_unix_ms(),
        )
    }

    pub(crate) fn cleanup_expired_empty_rooms_with_default_at_ms_and_collect(
        &self,
        default_empty_age_ms: i64,
        now_ms: i64,
    ) -> Result<Vec<proto::Room>, RoomStoreError> {
        let mut inner = self
            .inner
            .write()
            .map_err(|_| RoomStoreError::LockPoisoned)?;

        let mut removed_rooms = Vec::new();
        inner.rooms.retain(|_, record| {
            let Some(empty_since) = record.empty_since_unix_ms else {
                return true;
            };

            let elapsed_ms = now_ms.saturating_sub(empty_since);
            let room_timeout_seconds = if record.had_participants {
                if record.room.departure_timeout > 0 {
                    record.room.departure_timeout
                } else {
                    record.room.empty_timeout
                }
            } else {
                record.room.empty_timeout
            };
            let room_timeout_ms = if room_timeout_seconds > 0 {
                i64::from(room_timeout_seconds).saturating_mul(1000)
            } else {
                default_empty_age_ms
            };

            let keep_room = elapsed_ms < room_timeout_ms;
            if !keep_room {
                removed_rooms.push(record.room.clone());
            }
            keep_room
        });

        Ok(removed_rooms)
    }

    pub fn get_room(&self, room: &str) -> Result<proto::Room, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        inner
            .rooms
            .get(room)
            .map(room_snapshot)
            .ok_or(RoomStoreError::RoomNotFound)
    }

    /// Returns whether a room exists.
    pub fn room_exists(&self, room: &str) -> Result<bool, RoomStoreError> {
        let inner = self
            .inner
            .read()
            .map_err(|_| RoomStoreError::LockPoisoned)?;
        Ok(inner.rooms.contains_key(room))
    }

    /// Verifies that a room exists for room-scoped API operations that currently have no state effect.
    pub fn ensure_room_exists(&self, room: &str) -> Result<(), RoomStoreError> {
        if self.room_exists(room)? {
            return Ok(());
        }
        Err(RoomStoreError::RoomNotFound)
    }
}
