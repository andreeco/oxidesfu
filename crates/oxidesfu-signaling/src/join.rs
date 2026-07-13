use livekit_protocol as proto;
use oxidesfu_auth::AuthContext;
use prost::Message;

use crate::{
    relay::{
        NonLocalRelayJoinIntent, NonLocalRelayJoinResponse, inc_dispatch_attempts,
        inc_dispatch_failures,
    },
    state::SignalState,
};

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RoomNodePlacementOutcome {
    LocalHandling,
    NonLocalNeedsRelay { selected_room_node_id: String },
    DirectorySelectionFailed { error: String },
}

#[cfg(test)]
pub(crate) fn non_local_relay_rejection_details(
    response: &NonLocalRelayJoinResponse,
) -> Option<(String, String)> {
    match response {
        NonLocalRelayJoinResponse::Rejected { code, msg } => Some((code.clone(), msg.clone())),
        NonLocalRelayJoinResponse::Accepted { .. }
        | NonLocalRelayJoinResponse::AcceptedWithJoin { .. }
        | NonLocalRelayJoinResponse::AcceptedWithJoinAndSignals { .. } => None,
    }
}

pub(crate) fn non_local_relay_join_response_shape(
    response: &NonLocalRelayJoinResponse,
) -> Option<proto::JoinResponse> {
    match response {
        NonLocalRelayJoinResponse::Accepted {
            participant_sid,
            server_version,
            ping_interval,
            ping_timeout,
        } => Some(proto::JoinResponse {
            participant: Some(proto::ParticipantInfo {
                sid: participant_sid.clone(),
                ..Default::default()
            }),
            server_version: server_version.clone(),
            ping_interval: *ping_interval,
            ping_timeout: *ping_timeout,
            ..Default::default()
        }),
        NonLocalRelayJoinResponse::AcceptedWithJoin { join_response }
        | NonLocalRelayJoinResponse::AcceptedWithJoinAndSignals { join_response, .. } => {
            proto::JoinResponse::decode(join_response.as_slice()).ok()
        }
        NonLocalRelayJoinResponse::Rejected { .. } => None,
    }
}

/// Returns signal responses that must follow an accepted relayed join.
pub(crate) fn non_local_relay_initial_signal_responses(
    response: &NonLocalRelayJoinResponse,
) -> Vec<Vec<u8>> {
    match response {
        NonLocalRelayJoinResponse::AcceptedWithJoinAndSignals {
            initial_signal_responses,
            ..
        } => initial_signal_responses.clone(),
        NonLocalRelayJoinResponse::Accepted { .. }
        | NonLocalRelayJoinResponse::AcceptedWithJoin { .. }
        | NonLocalRelayJoinResponse::Rejected { .. } => Vec::new(),
    }
}

pub(crate) fn non_local_relay_intent_from_outcome(
    room_name: &str,
    auth: &AuthContext,
    request: &proto::JoinRequest,
    outcome: &RoomNodePlacementOutcome,
    subscriber_primary: bool,
) -> Option<NonLocalRelayJoinIntent> {
    match outcome {
        RoomNodePlacementOutcome::NonLocalNeedsRelay {
            selected_room_node_id,
        } => {
            let metadata = if request.metadata.is_empty() {
                auth.claims.metadata.clone()
            } else {
                request.metadata.clone()
            };
            let mut attributes = auth.claims.attributes.clone();
            attributes.extend(request.participant_attributes.clone());

            Some(NonLocalRelayJoinIntent {
                room: room_name.to_string(),
                identity: auth.participant_identity().to_string(),
                name: auth.claims.name.clone(),
                metadata,
                attributes,
                requested_participant_sid: if request.participant_sid.is_empty() {
                    None
                } else {
                    Some(request.participant_sid.clone())
                },
                selected_room_node_id: selected_room_node_id.clone(),
                subscriber_primary,
                can_publish: auth.claims.video.get_can_publish(),
                can_subscribe: auth.claims.video.get_can_subscribe(),
                can_publish_data: auth.claims.video.get_can_publish_data(),
                can_update_metadata: auth.claims.video.get_can_update_own_metadata(),
                hidden: auth.claims.video.hidden,
                api_key: auth.api_key.clone(),
                kind: auth.claims.kind.clone(),
                kind_details: auth.claims.kind_details.clone(),
                destination_room: auth.claims.video.destination_room.clone(),
                room_config: auth.claims.room_config.clone(),
            })
        }
        RoomNodePlacementOutcome::LocalHandling
        | RoomNodePlacementOutcome::DirectorySelectionFailed { .. } => None,
    }
}

pub(crate) enum RelayDispatchOutcome {
    NotAttempted,
    NoResponse,
    Responded(NonLocalRelayJoinResponse),
    Failed(String),
}

pub(crate) fn dispatch_non_local_relay_intent(
    state: &SignalState,
    room_name: &str,
    auth: &AuthContext,
    request: &proto::JoinRequest,
    outcome: &RoomNodePlacementOutcome,
    subscriber_primary: bool,
) -> RelayDispatchOutcome {
    inc_dispatch_attempts();
    if state.reject_non_local_room_placement {
        return RelayDispatchOutcome::NotAttempted;
    }

    let Some(relay_target) =
        non_local_relay_intent_from_outcome(room_name, auth, request, outcome, subscriber_primary)
    else {
        return RelayDispatchOutcome::NotAttempted;
    };

    tracing::debug!(
        room = %relay_target.room,
        identity = %relay_target.identity,
        selected_room_node_id = %relay_target.selected_room_node_id,
        "room_node_assignment_non_local_relay_target_metadata"
    );

    match state
        .non_local_relay_dispatcher
        .dispatch_non_local_join(relay_target)
    {
        Ok(Some(response)) => RelayDispatchOutcome::Responded(response),
        Ok(None) => RelayDispatchOutcome::NoResponse,
        Err(error) => {
            inc_dispatch_failures();
            RelayDispatchOutcome::Failed(error)
        }
    }
}

pub(crate) fn room_node_placement_outcome(
    state: &SignalState,
    room_name: &str,
) -> RoomNodePlacementOutcome {
    let Some(room_nodes) = state.room_nodes.as_ref() else {
        return RoomNodePlacementOutcome::LocalHandling;
    };

    match room_nodes.select_or_assign_node_for_room(room_name) {
        Ok(selected_node)
            if state
                .local_room_node_id
                .as_deref()
                .is_some_and(|local_id| local_id != selected_node.id) =>
        {
            RoomNodePlacementOutcome::NonLocalNeedsRelay {
                selected_room_node_id: selected_node.id,
            }
        }
        Ok(_) => RoomNodePlacementOutcome::LocalHandling,
        Err(err) => RoomNodePlacementOutcome::DirectorySelectionFailed {
            error: err.to_string(),
        },
    }
}
