// Shared test utilities and compatibility tests for OxideSFU.

/// Marker type for the compatibility test harness.
#[derive(Debug, Default, Clone, Copy)]
pub struct OxideSFUTestHarness;

#[cfg(test)]
#[allow(dead_code)]
pub(crate) mod support {
    use std::{
        collections::{HashMap, HashSet},
        io::ErrorKind,
        path::{Path, PathBuf},
        process::{Output, Stdio},
        sync::{Arc, Mutex},
        time::{Duration, SystemTime, UNIX_EPOCH},
    };

    use base64::{Engine, engine::general_purpose};
    use futures_util::{SinkExt, StreamExt};
    use livekit::{
        DataPacket, DataPacketKind, Room, RoomEvent, RoomOptions,
        options::TrackPublishOptions,
        prelude::{DataTrackFrame, LocalAudioTrack, LocalTrack, LocalVideoTrack, RtcAudioSource},
        webrtc::{
            audio_frame::AudioFrame,
            audio_source::native::NativeAudioSource,
            audio_stream::native::NativeAudioStream,
            prelude::AudioSourceOptions,
            video_source::{RtcVideoSource, VideoResolution, native::NativeVideoSource},
        },
    };
    use livekit_api::{
        access_token::{AccessToken, VideoGrants},
        services::room::{
            CreateRoomOptions, RoomClient, SendDataOptions, UpdateParticipantOptions,
        },
        signal_client::{SignalClient, SignalEvent, SignalOptions},
    };
    use livekit_protocol as proto;
    use oxidesfu_room::{
        RedisHashStore, RegisteredNode, RoomNodeDirectory, RoomNodeRegistry, RoomNodeRegistryError,
        RoomStoreError,
    };
    use oxidesfu_signaling::{
        NonLocalRelayDispatcher, NonLocalRelayJoinIntent, NonLocalRelayJoinResponse,
        NoopRelayIntentExecutionDriver, RedisMailboxRelayDispatcher, RedisRelayMailbox,
        RelayIntentExecutionDriver,
    };
    use prost::Message as _;
    use serde_json::Value as JsonValue;
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    use tokio_tungstenite::{
        connect_async,
        tungstenite::{Message, client::IntoClientRequest, http::HeaderValue},
    };

    const API_KEY: &str = "devkey";
    const API_SECRET: &str = "secret";

    include!("oxidesfu_process.rs");
    include!("livekit_yaml_process.rs");
    include!("go_livekit.rs");
    include!("ports.rs");
    include!("redis.rs");
    include!("tokens.rs");
    include!("websocket.rs");

    mod external_deployment {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/external_deployment.rs"
        ));
    }

    mod benchmark_load {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/benchmark/load.rs"
        ));
    }

    #[cfg(feature = "harness-e2e")]
    mod cli_room {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/cli/room.rs"));
    }
    #[cfg(feature = "harness-e2e")]
    mod cli_publish {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/cli/publish.rs"));
    }
    mod cli_misc {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/cli/misc.rs"));
    }

    #[cfg(feature = "harness-e2e")]
    mod differential_core {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/differential/core.rs"
        ));
    }
    mod differential_lite {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/differential/lite.rs"
        ));
    }
    #[cfg(feature = "harness-e2e")]
    mod differential_matrix {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/differential/matrix.rs"
        ));
    }
    mod differential_normalize {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/differential/normalize.rs"
        ));
    }

    #[cfg(feature = "harness-e2e")]
    mod distributed_churn {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/distributed/churn.rs"
        ));
    }
    mod distributed_relay_lifecycle {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/distributed/relay_lifecycle.rs"
        ));
    }
    #[cfg(feature = "harness-e2e")]
    mod distributed_two_process {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/distributed/two_process.rs"
        ));
    }

    #[cfg(feature = "harness-e2e")]
    mod probes_data {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/probes/data.rs"));
    }
    mod probes_media {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/probes/media.rs"));
    }
    mod probes_signaling {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/probes/signaling.rs"
        ));
    }
    mod probes_twirp {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/probes/twirp.rs"));
    }

    #[cfg(feature = "harness-e2e")]
    mod sdk_data {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/sdk/data.rs"));
    }
    #[cfg(feature = "harness-e2e")]
    mod sdk_media {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/sdk/media.rs"));
    }
    mod sdk_room {
        include!(concat!(env!("CARGO_MANIFEST_DIR"), "/src/sdk/room.rs"));
    }
    mod sdk_signal_client {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/sdk/signal_client.rs"
        ));
    }

    #[cfg(any(feature = "harness-e2e", feature = "upstream-livekit"))]
    #[allow(deprecated, unused_mut, dead_code)]
    #[allow(
        clippy::cmp_owned,
        clippy::collapsible_if,
        clippy::items_after_test_module,
        clippy::let_and_return,
        clippy::manual_is_multiple_of,
        clippy::same_item_push
    )]
    mod upstream_livekit {
        include!(concat!(
            env!("CARGO_MANIFEST_DIR"),
            "/src/upstream_livekit/mod.rs"
        ));
    }
}
