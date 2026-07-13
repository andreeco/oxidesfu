use std::collections::HashMap;

use super::{
    LIVEKIT_REALM, TurnAuthError, TurnAuthHandler, TurnMethod, TurnRequestAttributes,
    encode_base62_bytes, generate_turn_auth_key,
};

const TURN_TEST_API_KEY: &str = "APITestKey";
const TURN_TEST_API_SECRET: &str = "TestSecret";
const FIXED_NOW_UNIX: i64 = 1_700_000_000;

fn new_test_turn_auth_handler() -> TurnAuthHandler {
    TurnAuthHandler::new(HashMap::from([(
        TURN_TEST_API_KEY.to_string(),
        TURN_TEST_API_SECRET.to_string(),
    )]))
}

fn must_auth_creds(
    handler: &TurnAuthHandler,
    participant_id: &str,
    ttl_seconds: i64,
) -> (String, Vec<u8>) {
    let (username, expiry) = handler.create_username_at(
        TURN_TEST_API_KEY,
        participant_id,
        ttl_seconds,
        FIXED_NOW_UNIX,
    );
    let password = handler
        .create_password_at(TURN_TEST_API_KEY, participant_id, expiry, FIXED_NOW_UNIX)
        .expect("password should be created");

    let key = generate_turn_auth_key(&username, LIVEKIT_REALM, &password);
    (username, key)
}

fn all_turn_methods() -> [TurnMethod; 5] {
    [
        TurnMethod::Allocate,
        TurnMethod::Refresh,
        TurnMethod::CreatePermission,
        TurnMethod::ChannelBind,
        TurnMethod::Send,
    ]
}

#[test]
fn handle_auth_valid_credentials() {
    let handler = new_test_turn_auth_handler();
    let participant_id = "PA_valid";
    let (username, expected_key) = must_auth_creds(&handler, participant_id, 300);

    for method in all_turn_methods() {
        let decision = handler.handle_auth_at(
            TurnRequestAttributes {
                username: &username,
                method,
            },
            FIXED_NOW_UNIX,
        );

        assert!(decision.ok);
        assert_eq!(decision.user_id, participant_id);
        assert_eq!(decision.key, expected_key);
    }
}

#[test]
fn handle_auth_expired_allocate_rejected() {
    let handler = new_test_turn_auth_handler();
    let (username, _expiry) =
        handler.create_username_at(TURN_TEST_API_KEY, "PA_expired_alloc", -60, FIXED_NOW_UNIX);

    let decision = handler.handle_auth_at(
        TurnRequestAttributes {
            username: &username,
            method: TurnMethod::Allocate,
        },
        FIXED_NOW_UNIX,
    );

    assert!(!decision.ok);
}

#[test]
fn handle_auth_expired_non_allocate_allowed() {
    let handler = new_test_turn_auth_handler();
    let participant_id = "PA_expired_refresh";

    let (username, expiry) =
        handler.create_username_at(TURN_TEST_API_KEY, participant_id, -60, FIXED_NOW_UNIX);

    let password = handler
        .compute_password(TURN_TEST_API_KEY, participant_id, expiry)
        .expect("compute_password should work for expired usernames");
    let expected_key = generate_turn_auth_key(&username, LIVEKIT_REALM, &password);

    for method in [
        TurnMethod::Refresh,
        TurnMethod::CreatePermission,
        TurnMethod::ChannelBind,
        TurnMethod::Send,
    ] {
        let decision = handler.handle_auth_at(
            TurnRequestAttributes {
                username: &username,
                method,
            },
            FIXED_NOW_UNIX,
        );

        assert!(decision.ok);
        assert_eq!(decision.user_id, participant_id);
        assert_eq!(decision.key, expected_key);
    }
}

#[test]
fn handle_auth_wrong_username_rejected() {
    let handler = new_test_turn_auth_handler();

    let decision = handler.handle_auth_at(
        TurnRequestAttributes {
            username: "not-base62!!!",
            method: TurnMethod::Refresh,
        },
        FIXED_NOW_UNIX,
    );

    assert!(!decision.ok);
}

#[test]
fn handle_auth_two_part_username_rejected() {
    let handler = new_test_turn_auth_handler();

    let username = encode_base62_bytes(format!("{TURN_TEST_API_KEY}|PA_two_part").as_bytes());

    for method in all_turn_methods() {
        let decision = handler.handle_auth_at(
            TurnRequestAttributes {
                username: &username,
                method,
            },
            FIXED_NOW_UNIX,
        );
        assert!(!decision.ok);
    }
}

#[test]
fn handle_auth_zero_expiry_rejected() {
    let handler = new_test_turn_auth_handler();

    let username = encode_base62_bytes(format!("{TURN_TEST_API_KEY}|PA_zero_expiry|0").as_bytes());

    for method in all_turn_methods() {
        let decision = handler.handle_auth_at(
            TurnRequestAttributes {
                username: &username,
                method,
            },
            FIXED_NOW_UNIX,
        );
        assert!(!decision.ok);
    }
}

#[test]
fn parse_username_two_part_rejected() {
    let handler = new_test_turn_auth_handler();

    let username = encode_base62_bytes(format!("{TURN_TEST_API_KEY}|PA_parse_two_part").as_bytes());

    let parsed = handler.parse_username(&username);
    assert!(parsed.is_err());
}

#[test]
fn parse_username_zero_expiry_rejected() {
    let handler = new_test_turn_auth_handler();

    let username =
        encode_base62_bytes(format!("{TURN_TEST_API_KEY}|PA_parse_zero_expiry|0").as_bytes());

    let parsed = handler.parse_username(&username);
    assert!(matches!(parsed, Err(TurnAuthError::Expired)));
}

#[test]
fn create_password_zero_expiry_rejected() {
    let handler = new_test_turn_auth_handler();

    let result = handler.create_password_at(
        TURN_TEST_API_KEY,
        "PA_password_zero_expiry",
        0,
        FIXED_NOW_UNIX,
    );

    assert!(matches!(result, Err(TurnAuthError::Expired)));
}
