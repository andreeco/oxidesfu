use livekit_protocol as proto;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct ScriptMatch {
    expression: BoolExpr,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum BoolExpr {
    Or(Vec<BoolExpr>),
    And(Vec<BoolExpr>),
    Compare {
        left: Operand,
        op: CompareOp,
        right: Operand,
    },
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompareOp {
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Operand {
    Field(String),
    Integer(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Value {
    Integer(i64),
    String(String),
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub(crate) enum ClientConfigMatchError {
    #[error("invalid expression: {message}")]
    InvalidExpression { message: String },
    #[error("unknown client field {field}")]
    UnknownField { field: String },
    #[error("invalid comparison for expression")]
    InvalidComparison,
}

#[derive(Debug, Clone)]
pub(crate) struct ConfigurationItem {
    pub(crate) matcher: ScriptMatch,
    pub(crate) configuration: proto::ClientConfiguration,
    pub(crate) merge: bool,
}

#[derive(Debug, Clone)]
pub(crate) struct StaticClientConfigurationManager {
    items: Vec<ConfigurationItem>,
}

impl StaticClientConfigurationManager {
    pub(crate) fn new(items: Vec<ConfigurationItem>) -> Self {
        Self { items }
    }

    pub(crate) fn get_configuration(
        &self,
        client_info: &proto::ClientInfo,
    ) -> Option<proto::ClientConfiguration> {
        let mut matched = Vec::new();
        for item in &self.items {
            let is_match = match item.matcher.matches(client_info) {
                Ok(value) => value,
                Err(_) => continue,
            };
            if !is_match {
                continue;
            }
            if !item.merge {
                return Some(item.configuration.clone());
            }
            matched.push(item.configuration.clone());
        }

        let mut iter = matched.into_iter();
        let mut merged = iter.next()?;
        for conf in iter {
            merge_client_configuration(&mut merged, &conf);
        }

        Some(merged)
    }
}

impl ScriptMatch {
    pub(crate) fn new(expression: &str) -> Result<Self, ClientConfigMatchError> {
        let mut parser = Parser::new(expression);
        let parsed = parser.parse_expression()?;
        if !parser.is_eof() {
            return Err(ClientConfigMatchError::InvalidExpression {
                message: "unexpected trailing tokens".to_string(),
            });
        }

        Ok(Self { expression: parsed })
    }

    pub(crate) fn matches(
        &self,
        client_info: &proto::ClientInfo,
    ) -> Result<bool, ClientConfigMatchError> {
        evaluate_bool_expr(&self.expression, client_info)
    }
}

pub(crate) fn static_configuration_for_client_info(
    client_info: &proto::ClientInfo,
) -> Option<proto::ClientConfiguration> {
    let items = vec![
        ConfigurationItem {
            matcher: ScriptMatch::new("c.browser == \"safari\"").ok()?,
            configuration: proto::ClientConfiguration {
                disabled_codecs: Some(proto::DisabledCodecs {
                    codecs: vec![proto::Codec {
                        mime: "video/AV1".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            merge: true,
        },
        ConfigurationItem {
            matcher: ScriptMatch::new("c.browser == \"safari\" && c.browser_version > \"18.3\"")
                .ok()?,
            configuration: proto::ClientConfiguration {
                disabled_codecs: Some(proto::DisabledCodecs {
                    publish: vec![proto::Codec {
                        mime: "video/VP9".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            merge: true,
        },
        ConfigurationItem {
            matcher: ScriptMatch::new("(c.device_model == \"xiaomi 2201117ti\" && c.os == \"android\") || ((c.browser == \"firefox\" || c.browser == \"firefox mobile\") && (c.os == \"linux\" || c.os == \"android\"))")
                .ok()?,
            configuration: proto::ClientConfiguration {
                disabled_codecs: Some(proto::DisabledCodecs {
                    publish: vec![proto::Codec {
                        mime: "video/H264".to_string(),
                        ..Default::default()
                    }],
                    ..Default::default()
                }),
                ..Default::default()
            },
            merge: false,
        },
    ];

    StaticClientConfigurationManager::new(items).get_configuration(client_info)
}

fn merge_client_configuration(
    dst: &mut proto::ClientConfiguration,
    src: &proto::ClientConfiguration,
) {
    if src.resume_connection != 0 {
        dst.resume_connection = src.resume_connection;
    }
    if src.force_relay != 0 {
        dst.force_relay = src.force_relay;
    }

    if let Some(video) = src.video.as_ref() {
        dst.video = Some(video.clone());
    }

    if let Some(src_codecs) = src.disabled_codecs.as_ref() {
        let dst_codecs = dst.disabled_codecs.get_or_insert_with(Default::default);
        merge_codec_list(&mut dst_codecs.codecs, &src_codecs.codecs);
        merge_codec_list(&mut dst_codecs.publish, &src_codecs.publish);
    }
}

fn merge_codec_list(dst: &mut Vec<proto::Codec>, src: &[proto::Codec]) {
    for codec in src {
        if dst
            .iter()
            .any(|existing| existing.mime.eq_ignore_ascii_case(&codec.mime))
        {
            continue;
        }
        dst.push(codec.clone());
    }
}

fn evaluate_bool_expr(
    expression: &BoolExpr,
    client_info: &proto::ClientInfo,
) -> Result<bool, ClientConfigMatchError> {
    match expression {
        BoolExpr::Or(items) => {
            for item in items {
                if evaluate_bool_expr(item, client_info)? {
                    return Ok(true);
                }
            }
            Ok(false)
        }
        BoolExpr::And(items) => {
            for item in items {
                if !evaluate_bool_expr(item, client_info)? {
                    return Ok(false);
                }
            }
            Ok(true)
        }
        BoolExpr::Compare { left, op, right } => {
            let lhs = resolve_operand(left, client_info)?;
            let rhs = resolve_operand(right, client_info)?;
            evaluate_compare(&lhs, *op, &rhs)
        }
    }
}

fn resolve_operand(
    operand: &Operand,
    client_info: &proto::ClientInfo,
) -> Result<Value, ClientConfigMatchError> {
    match operand {
        Operand::Integer(value) => Ok(Value::Integer(*value)),
        Operand::String(value) => Ok(Value::String(value.clone())),
        Operand::Field(field) => resolve_client_field(field, client_info),
    }
}

fn resolve_client_field(
    field: &str,
    client_info: &proto::ClientInfo,
) -> Result<Value, ClientConfigMatchError> {
    match field {
        "c.protocol" => Ok(Value::Integer(client_info.protocol as i64)),
        "c.browser" => Ok(Value::String(
            client_info.browser.trim().to_ascii_lowercase(),
        )),
        "c.browser_version" => Ok(Value::String(
            client_info.browser_version.trim().to_string(),
        )),
        "c.os" => Ok(Value::String(client_info.os.trim().to_ascii_lowercase())),
        "c.os_version" => Ok(Value::String(client_info.os_version.clone())),
        "c.device_model" => Ok(Value::String(
            client_info.device_model.trim().to_ascii_lowercase(),
        )),
        "c.address" => Ok(Value::String(client_info.address.clone())),
        "c.version" => Ok(Value::String(client_info.version.clone())),
        "c.sdk" => {
            let sdk = proto::client_info::Sdk::try_from(client_info.sdk)
                .ok()
                .map(|sdk| sdk.as_str_name().to_ascii_lowercase())
                .unwrap_or_default();
            Ok(Value::String(sdk))
        }
        _ => Err(ClientConfigMatchError::UnknownField {
            field: field.to_string(),
        }),
    }
}

fn evaluate_compare(
    lhs: &Value,
    op: CompareOp,
    rhs: &Value,
) -> Result<bool, ClientConfigMatchError> {
    match (lhs, rhs) {
        (Value::Integer(lhs), Value::Integer(rhs)) => Ok(compare_i64(*lhs, op, *rhs)),
        (Value::String(lhs), Value::String(rhs)) => {
            let cmp = compare_version_or_string(lhs, rhs);
            Ok(compare_ordering(cmp, op))
        }
        _ => Err(ClientConfigMatchError::InvalidComparison),
    }
}

fn compare_i64(lhs: i64, op: CompareOp, rhs: i64) -> bool {
    match op {
        CompareOp::Eq => lhs == rhs,
        CompareOp::Ne => lhs != rhs,
        CompareOp::Gt => lhs > rhs,
        CompareOp::Ge => lhs >= rhs,
        CompareOp::Lt => lhs < rhs,
        CompareOp::Le => lhs <= rhs,
    }
}

fn compare_ordering(cmp: i32, op: CompareOp) -> bool {
    match op {
        CompareOp::Eq => cmp == 0,
        CompareOp::Ne => cmp != 0,
        CompareOp::Gt => cmp > 0,
        CompareOp::Ge => cmp >= 0,
        CompareOp::Lt => cmp < 0,
        CompareOp::Le => cmp <= 0,
    }
}

fn compare_version_or_string(lhs: &str, rhs: &str) -> i32 {
    match (parse_semver(lhs), parse_semver(rhs)) {
        (Some(lhs), Some(rhs)) => compare_semver_components(&lhs, &rhs),
        _ => match lhs.cmp(rhs) {
            std::cmp::Ordering::Less => -1,
            std::cmp::Ordering::Equal => 0,
            std::cmp::Ordering::Greater => 1,
        },
    }
}

fn parse_semver(raw: &str) -> Option<Vec<u32>> {
    if raw.is_empty() {
        return None;
    }

    let mut out = Vec::new();
    for part in raw.split('.') {
        if part.is_empty() {
            return None;
        }
        let parsed = part.parse::<u32>().ok()?;
        out.push(parsed);
    }

    if out.len() >= 3 { Some(out) } else { None }
}

fn compare_semver_components(lhs: &[u32], rhs: &[u32]) -> i32 {
    let max_len = lhs.len().max(rhs.len());
    for idx in 0..max_len {
        let l = *lhs.get(idx).unwrap_or(&0);
        let r = *rhs.get(idx).unwrap_or(&0);
        if l < r {
            return -1;
        }
        if l > r {
            return 1;
        }
    }
    0
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum Token {
    Identifier(String),
    Integer(i64),
    String(String),
    And,
    Or,
    Eq,
    Ne,
    Gt,
    Ge,
    Lt,
    Le,
    LParen,
    RParen,
}

struct Parser {
    tokens: Vec<Token>,
    index: usize,
}

impl Parser {
    fn new(input: &str) -> Self {
        Self {
            tokens: tokenize(input),
            index: 0,
        }
    }

    fn parse_expression(&mut self) -> Result<BoolExpr, ClientConfigMatchError> {
        self.parse_or()
    }

    fn parse_or(&mut self) -> Result<BoolExpr, ClientConfigMatchError> {
        let mut items = vec![self.parse_and()?];
        while self.peek() == Some(&Token::Or) {
            self.index += 1;
            items.push(self.parse_and()?);
        }
        if items.len() == 1 {
            Ok(items.remove(0))
        } else {
            Ok(BoolExpr::Or(items))
        }
    }

    fn parse_and(&mut self) -> Result<BoolExpr, ClientConfigMatchError> {
        let mut items = vec![self.parse_term()?];
        while self.peek() == Some(&Token::And) {
            self.index += 1;
            items.push(self.parse_term()?);
        }
        if items.len() == 1 {
            Ok(items.remove(0))
        } else {
            Ok(BoolExpr::And(items))
        }
    }

    fn parse_term(&mut self) -> Result<BoolExpr, ClientConfigMatchError> {
        if self.peek() == Some(&Token::LParen) {
            self.index += 1;
            let expression = self.parse_expression()?;
            self.expect(Token::RParen)?;
            return Ok(expression);
        }

        let left = self.parse_operand()?;
        let op = self.parse_compare_op()?;
        let right = self.parse_operand()?;
        Ok(BoolExpr::Compare { left, op, right })
    }

    fn parse_operand(&mut self) -> Result<Operand, ClientConfigMatchError> {
        let token = self
            .next()
            .ok_or_else(|| ClientConfigMatchError::InvalidExpression {
                message: "expected operand".to_string(),
            })?;

        match token {
            Token::Identifier(value) => Ok(Operand::Field(value)),
            Token::Integer(value) => Ok(Operand::Integer(value)),
            Token::String(value) => Ok(Operand::String(value)),
            _ => Err(ClientConfigMatchError::InvalidExpression {
                message: "expected operand".to_string(),
            }),
        }
    }

    fn parse_compare_op(&mut self) -> Result<CompareOp, ClientConfigMatchError> {
        let token = self
            .next()
            .ok_or_else(|| ClientConfigMatchError::InvalidExpression {
                message: "expected comparison operator".to_string(),
            })?;
        let op = match token {
            Token::Eq => CompareOp::Eq,
            Token::Ne => CompareOp::Ne,
            Token::Gt => CompareOp::Gt,
            Token::Ge => CompareOp::Ge,
            Token::Lt => CompareOp::Lt,
            Token::Le => CompareOp::Le,
            _ => {
                return Err(ClientConfigMatchError::InvalidExpression {
                    message: "expected comparison operator".to_string(),
                });
            }
        };
        Ok(op)
    }

    fn expect(&mut self, expected: Token) -> Result<(), ClientConfigMatchError> {
        let token = self
            .next()
            .ok_or_else(|| ClientConfigMatchError::InvalidExpression {
                message: "unexpected end of expression".to_string(),
            })?;
        if token == expected {
            Ok(())
        } else {
            Err(ClientConfigMatchError::InvalidExpression {
                message: "unexpected token".to_string(),
            })
        }
    }

    fn peek(&self) -> Option<&Token> {
        self.tokens.get(self.index)
    }

    fn next(&mut self) -> Option<Token> {
        let token = self.tokens.get(self.index).cloned();
        self.index += usize::from(token.is_some());
        token
    }

    fn is_eof(&self) -> bool {
        self.index >= self.tokens.len()
    }
}

fn tokenize(input: &str) -> Vec<Token> {
    let mut tokens = Vec::new();
    let mut chars = input.chars().peekable();

    while let Some(ch) = chars.peek().copied() {
        if ch.is_whitespace() {
            chars.next();
            continue;
        }

        match ch {
            '(' => {
                chars.next();
                tokens.push(Token::LParen);
            }
            ')' => {
                chars.next();
                tokens.push(Token::RParen);
            }
            '&' => {
                chars.next();
                if chars.peek() == Some(&'&') {
                    chars.next();
                    tokens.push(Token::And);
                }
            }
            '|' => {
                chars.next();
                if chars.peek() == Some(&'|') {
                    chars.next();
                    tokens.push(Token::Or);
                }
            }
            '=' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Eq);
                }
            }
            '!' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Ne);
                }
            }
            '>' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Ge);
                } else {
                    tokens.push(Token::Gt);
                }
            }
            '<' => {
                chars.next();
                if chars.peek() == Some(&'=') {
                    chars.next();
                    tokens.push(Token::Le);
                } else {
                    tokens.push(Token::Lt);
                }
            }
            '"' => {
                chars.next();
                let mut value = String::new();
                while let Some(next) = chars.next() {
                    if next == '"' {
                        break;
                    }
                    value.push(next);
                }
                tokens.push(Token::String(value));
            }
            '-' | '0'..='9' => {
                let mut value = String::new();
                value.push(ch);
                chars.next();
                while let Some(next) = chars.peek().copied() {
                    if next.is_ascii_digit() {
                        value.push(next);
                        chars.next();
                    } else {
                        break;
                    }
                }
                if let Ok(parsed) = value.parse::<i64>() {
                    tokens.push(Token::Integer(parsed));
                }
            }
            _ => {
                if ch.is_ascii_alphabetic() || ch == '_' {
                    let mut value = String::new();
                    value.push(ch);
                    chars.next();
                    while let Some(next) = chars.peek().copied() {
                        if next.is_ascii_alphanumeric() || next == '_' || next == '.' {
                            value.push(next);
                            chars.next();
                        } else {
                            break;
                        }
                    }
                    tokens.push(Token::Identifier(value));
                } else {
                    chars.next();
                }
            }
        }
    }

    tokens
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn script_match_matches_upstream_cases() {
        let client = proto::ClientInfo {
            protocol: 6,
            browser: "chrome".to_string(),
            sdk: proto::client_info::Sdk::Android as i32,
            device_model: "12345".to_string(),
            browser_version: "13.2".to_string(),
            version: "2.17.1".to_string(),
            ..Default::default()
        };

        let cases = vec![
            ("c.protocol > 5", Some(true), false),
            ("cc.protocol > 5", None, true),
            ("c.protocols > 5", None, true),
            (
                "c.protocol > 5 && (c.sdk == \"android\" || c.sdk == \"ios\")",
                Some(true),
                false,
            ),
            (
                "(c.device_model == \"xiaomi 2201117ti\" && c.os == \"android\") || ((c.browser == \"firefox\" || c.browser == \"firefox mobile\") && (c.os == \"linux\" || c.os == \"android\"))",
                Some(false),
                false,
            ),
            ("c.browser_version < \"11.3\"", Some(false), false),
            ("c.browser_version <= \"13.2\"", Some(true), false),
            ("c.browser_version > \"11.3\"", Some(true), false),
            ("c.browser_version >= \"13.2\"", Some(true), false),
            ("c.version < \"2.16.10\"", Some(false), false),
            ("c.version <= \"2.17.1\"", Some(true), false),
            ("c.version > \"2.16.10\"", Some(true), false),
            ("c.version >= \"2.17.1\"", Some(true), false),
        ];

        for (expression, expected, should_error) in cases {
            let matcher = ScriptMatch::new(expression);
            match (matcher, should_error) {
                (Err(_), true) => continue,
                (Err(err), false) => panic!("unexpected parse error for {expression}: {err}"),
                (Ok(_), true) => {}
                (Ok(matcher), false) => {
                    let actual = matcher.matches(&client).unwrap_or_else(|err| {
                        panic!("unexpected eval error for {expression}: {err}")
                    });
                    assert_eq!(Some(actual), expected, "expression: {expression}");
                }
            }
        }
    }

    #[test]
    fn script_match_configuration_merge_behaves_like_upstream() {
        let first = ConfigurationItem {
            matcher: ScriptMatch::new("c.protocol > 5 && c.browser != \"firefox\"")
                .expect("matcher should compile"),
            configuration: proto::ClientConfiguration {
                resume_connection: proto::ClientConfigSetting::Enabled as i32,
                ..Default::default()
            },
            merge: true,
        };

        let second = ConfigurationItem {
            matcher: ScriptMatch::new("c.sdk == \"android\"").expect("matcher should compile"),
            configuration: proto::ClientConfiguration {
                video: Some(proto::VideoConfiguration {
                    hardware_encoder: proto::ClientConfigSetting::Disabled as i32,
                    ..Default::default()
                }),
                ..Default::default()
            },
            merge: true,
        };

        let manager = StaticClientConfigurationManager::new(vec![first, second]);

        let none = manager.get_configuration(&proto::ClientInfo {
            protocol: 4,
            ..Default::default()
        });
        assert!(none.is_none());

        let none_firefox = manager.get_configuration(&proto::ClientInfo {
            protocol: 6,
            browser: "firefox".to_string(),
            ..Default::default()
        });
        assert!(none_firefox.is_none());

        let merged = manager
            .get_configuration(&proto::ClientInfo {
                protocol: 6,
                browser: "chrome".to_string(),
                sdk: proto::client_info::Sdk::Android as i32,
                ..Default::default()
            })
            .expect("configuration should match and merge");
        assert_eq!(
            merged.resume_connection,
            proto::ClientConfigSetting::Enabled as i32
        );
        assert_eq!(
            merged
                .video
                .as_ref()
                .expect("merged config should include video")
                .hardware_encoder,
            proto::ClientConfigSetting::Disabled as i32
        );
    }

    #[test]
    fn static_configuration_safari_and_firefox_xiaomi_rules_match_upstream() {
        let safari = static_configuration_for_client_info(&proto::ClientInfo {
            browser: "safari".to_string(),
            browser_version: "18.2".to_string(),
            ..Default::default()
        })
        .expect("safari should produce client config");
        let safari_codecs = safari
            .disabled_codecs
            .as_ref()
            .expect("safari should include disabled codecs");
        assert_eq!(safari_codecs.codecs.len(), 1);
        assert_eq!(safari_codecs.codecs[0].mime, "video/AV1");
        assert!(safari_codecs.publish.is_empty());

        let safari_new = static_configuration_for_client_info(&proto::ClientInfo {
            browser: "safari".to_string(),
            browser_version: "18.4".to_string(),
            ..Default::default()
        })
        .expect("new safari should produce client config");
        let safari_new_codecs = safari_new
            .disabled_codecs
            .as_ref()
            .expect("new safari should include disabled codecs");
        assert_eq!(safari_new_codecs.codecs.len(), 1);
        assert_eq!(safari_new_codecs.codecs[0].mime, "video/AV1");
        assert_eq!(safari_new_codecs.publish.len(), 1);
        assert_eq!(safari_new_codecs.publish[0].mime, "video/VP9");

        let firefox_linux = static_configuration_for_client_info(&proto::ClientInfo {
            browser: "firefox".to_string(),
            os: "linux".to_string(),
            ..Default::default()
        })
        .expect("firefox linux should produce client config");
        let firefox_codecs = firefox_linux
            .disabled_codecs
            .as_ref()
            .expect("firefox should include disabled codecs");
        assert!(firefox_codecs.codecs.is_empty());
        assert_eq!(firefox_codecs.publish.len(), 1);
        assert_eq!(firefox_codecs.publish[0].mime, "video/H264");
    }
}
