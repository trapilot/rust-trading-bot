use super::{BASE_URL_TCBS_REST, BASE_URL_TCBS_STREAM};
use chrono::{DateTime, TimeDelta, Utc};
use serde::Deserialize;
use serde_json::json;
use std::{
    fmt::{Debug, Formatter},
    sync::OnceLock,
    time::Duration,
};
use tokio::sync::Mutex;
use tracing::{debug, info};
use trading_bot_integration::error::SocketError;
use url::Url;

/// Env var holding a ready-to-use TCBS Open API JWT (preferred for unattended runs).
pub const ENV_TCBS_TOKEN: &str = "TCBS_TOKEN";

/// Env var holding the TCBS API key (generated on TCInvest). Used with [`ENV_TCBS_OTP`] when
/// [`ENV_TCBS_TOKEN`] is not set.
pub const ENV_TCBS_API_KEY: &str = "TCBS_API_KEY";

/// Env var holding a Smart OTP (iOTP) from the TCInvest app, exchanged with the API key for a JWT.
pub const ENV_TCBS_OTP: &str = "TCBS_OTP";

/// Env var overriding the REST base url (optional, defaults to [`BASE_URL_TCBS_REST`]).
pub const ENV_TCBS_REST_URL: &str = "TCBS_REST_URL";

/// Env var overriding the price board WebSocket url (optional, defaults to [`BASE_URL_TCBS_STREAM`]).
pub const ENV_TCBS_STREAM_URL: &str = "TCBS_STREAM_URL";

/// Path (relative to the REST base url) of the API key -> JWT exchange endpoint.
///
/// See docs: <https://developers.tcbs.com.vn/docs/v1.0.0/auth/token/>
pub const TCBS_TOKEN_PATH: &str = "gaia/v1/oauth2/openapi/token";

/// Maximum lifetime of a TCBS Open API JWT, used when the token's `exp` claim is unreadable.
pub const TCBS_TOKEN_MAX_LIFETIME: TimeDelta = TimeDelta::hours(8);

/// A cached token is treated as expired this long before it actually expires.
const TCBS_TOKEN_EXPIRY_MARGIN: TimeDelta = TimeDelta::minutes(5);

/// Timeout for the token exchange HTTP request.
const TCBS_HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// How the TCBS Open API JWT is obtained.
#[derive(Clone, PartialEq, Eq)]
pub enum TcbsCredentials {
    /// A JWT obtained elsewhere (eg/ via the token endpoint or the TCBS docs playground).
    Token(String),
    /// An API key plus a Smart OTP, exchanged for a JWT on first connect.
    ///
    /// The OTP comes from the TCInvest app and cannot be generated unattended, so the resulting
    /// JWT is cached for the lifetime of the process and reused on every reconnect until it
    /// expires (max 8 hours).
    ApiKey { api_key: String, otp: String },
}

impl Debug for TcbsCredentials {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            TcbsCredentials::Token(_) => f.write_str("Token(<redacted>)"),
            TcbsCredentials::ApiKey { .. } => {
                f.write_str("ApiKey { api_key: <redacted>, otp: <redacted> }")
            }
        }
    }
}

/// Runtime configuration for connecting to the TCBS Open API.
///
/// The [`Connector`](crate::exchange::Connector) interface is static (no instance state), so the
/// configuration is read from the environment each time a connection is (re)established.
#[derive(Clone, Debug)]
pub struct TcbsConfig {
    pub credentials: TcbsCredentials,
    pub rest_url: Url,
    pub stream_url: Url,
}

impl TcbsConfig {
    /// Build a [`TcbsConfig`] from the `TCBS_*` environment variables, using
    /// `default_stream_url` when `TCBS_STREAM_URL` is unset.
    pub fn from_env(default_stream_url: Url) -> Result<Self, SocketError> {
        Self::from_lookup(|key| std::env::var(key).ok(), default_stream_url)
    }

    fn from_lookup<F>(lookup: F, default_stream_url: Url) -> Result<Self, SocketError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let non_empty = |key: &str| lookup(key).filter(|value| !value.trim().is_empty());

        let credentials = match (
            non_empty(ENV_TCBS_TOKEN),
            non_empty(ENV_TCBS_API_KEY),
            non_empty(ENV_TCBS_OTP),
        ) {
            (Some(token), _, _) => TcbsCredentials::Token(token.trim().to_string()),
            (None, Some(api_key), Some(otp)) => TcbsCredentials::ApiKey {
                api_key: api_key.trim().to_string(),
                otp: otp.trim().to_string(),
            },
            _ => {
                return Err(SocketError::Subscribe(format!(
                    "TCBS requires either {ENV_TCBS_TOKEN}, or {ENV_TCBS_API_KEY} and {ENV_TCBS_OTP}"
                )));
            }
        };

        let rest_url = match non_empty(ENV_TCBS_REST_URL) {
            Some(url) => parse_base_url(&url)?,
            None => Url::parse(BASE_URL_TCBS_REST)?,
        };
        let stream_url = match non_empty(ENV_TCBS_STREAM_URL) {
            Some(url) => Url::parse(url.trim())?,
            None => default_stream_url,
        };

        Ok(Self {
            credentials,
            rest_url,
            stream_url,
        })
    }
}

/// Default price board url as a [`Url`].
pub fn default_stream_url() -> Result<Url, url::ParseError> {
    Url::parse(BASE_URL_TCBS_STREAM)
}

/// Parse a base url, ensuring a trailing '/' so that [`Url::join`] appends to it.
fn parse_base_url(url: &str) -> Result<Url, SocketError> {
    let url = url.trim();
    let url = if url.ends_with('/') {
        Url::parse(url)
    } else {
        Url::parse(&format!("{url}/"))
    };
    url.map_err(SocketError::from)
}

/// JWT obtained from an API key + OTP exchange, cached across reconnects.
#[derive(Clone)]
struct CachedToken {
    api_key: String,
    token: String,
    expires_at: DateTime<Utc>,
}

impl CachedToken {
    fn is_usable_for(&self, api_key: &str, now: DateTime<Utc>) -> bool {
        self.api_key == api_key && now + TCBS_TOKEN_EXPIRY_MARGIN < self.expires_at
    }
}

fn token_cache() -> &'static Mutex<Option<CachedToken>> {
    static CACHE: OnceLock<Mutex<Option<CachedToken>>> = OnceLock::new();
    CACHE.get_or_init(|| Mutex::new(None))
}

/// Resolve the JWT to authenticate the WebSocket with.
///
/// - [`TcbsCredentials::Token`] is returned as is.
/// - [`TcbsCredentials::ApiKey`] is exchanged once and the JWT cached process-wide (the lock is
///   held during the exchange so concurrent connections do not spend the same OTP twice).
pub async fn access_token(config: &TcbsConfig) -> Result<String, SocketError> {
    let (api_key, otp) = match &config.credentials {
        TcbsCredentials::Token(token) => return Ok(token.clone()),
        TcbsCredentials::ApiKey { api_key, otp } => (api_key, otp),
    };

    let mut cache = token_cache().lock().await;
    if let Some(cached) = cache
        .as_ref()
        .filter(|cached| cached.is_usable_for(api_key, Utc::now()))
    {
        debug!(expires_at = %cached.expires_at, "reusing cached TCBS Open API token");
        return Ok(cached.token.clone());
    }

    let token = exchange_api_key(&config.rest_url, api_key, otp).await?;
    let expires_at = jwt_expiry(&token).unwrap_or_else(|| Utc::now() + TCBS_TOKEN_MAX_LIFETIME);
    info!(%expires_at, "obtained TCBS Open API token");

    *cache = Some(CachedToken {
        api_key: api_key.clone(),
        token: token.clone(),
        expires_at,
    });
    Ok(token)
}

/// Drop the cached JWT, eg/ after the server rejected it.
pub async fn invalidate_cached_token() {
    token_cache().lock().await.take();
}

#[derive(Deserialize)]
struct TcbsTokenResponse {
    #[serde(default)]
    token: Option<String>,
}

async fn exchange_api_key(rest_url: &Url, api_key: &str, otp: &str) -> Result<String, SocketError> {
    let http = reqwest::Client::builder()
        .timeout(TCBS_HTTP_TIMEOUT)
        .build()
        .map_err(SocketError::Http)?;

    let response = http
        .post(rest_url.join(TCBS_TOKEN_PATH)?)
        .json(&json!({ "apiKey": api_key, "otp": otp }))
        .send()
        .await
        .map_err(SocketError::Http)?;
    let status = response.status();
    let payload = response.text().await.map_err(SocketError::Http)?;
    if !status.is_success() {
        return Err(SocketError::HttpResponse(
            status,
            format!(
                "TCBS token exchange failed (the OTP may be expired or already used): {payload}"
            ),
        ));
    }

    parse_token_response(payload)
}

fn parse_token_response(payload: String) -> Result<String, SocketError> {
    let response = serde_json::from_str::<TcbsTokenResponse>(&payload)
        .map_err(|error| SocketError::Deserialise { error, payload })?;
    response
        .token
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            SocketError::Subscribe("TCBS token response did not contain a token".to_string())
        })
}

/// Read the `exp` claim (epoch seconds) of a JWT without verifying it.
fn jwt_expiry(token: &str) -> Option<DateTime<Utc>> {
    use base64::Engine;

    #[derive(Deserialize)]
    struct Claims {
        exp: i64,
    }

    let payload = token.split('.').nth(1)?;
    let decoded = base64::engine::general_purpose::URL_SAFE_NO_PAD
        .decode(payload.trim_end_matches('='))
        .ok()?;
    let claims = serde_json::from_slice::<Claims>(&decoded).ok()?;
    DateTime::from_timestamp(claims.exp, 0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use base64::Engine;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| vars.get(key).cloned()
    }

    fn stream_url() -> Url {
        default_stream_url().unwrap()
    }

    #[test]
    fn test_tcbs_config_token_takes_precedence() {
        let config = TcbsConfig::from_lookup(
            lookup(&[
                (ENV_TCBS_TOKEN, "jwt"),
                (ENV_TCBS_API_KEY, "key"),
                (ENV_TCBS_OTP, "123456"),
            ]),
            stream_url(),
        )
        .unwrap();

        assert_eq!(
            config.credentials,
            TcbsCredentials::Token("jwt".to_string())
        );
        assert_eq!(config.rest_url.as_str(), BASE_URL_TCBS_REST);
        assert_eq!(config.stream_url.as_str(), BASE_URL_TCBS_STREAM);
        assert!(!format!("{config:?}").contains("jwt\""));
    }

    #[test]
    fn test_tcbs_config_api_key_and_overrides() {
        let config = TcbsConfig::from_lookup(
            lookup(&[
                (ENV_TCBS_API_KEY, "key"),
                (ENV_TCBS_OTP, "123456"),
                (ENV_TCBS_REST_URL, "https://example.com/api"),
                (ENV_TCBS_STREAM_URL, "ws://localhost:9000/stream"),
            ]),
            stream_url(),
        )
        .unwrap();

        assert_eq!(
            config.credentials,
            TcbsCredentials::ApiKey {
                api_key: "key".to_string(),
                otp: "123456".to_string()
            }
        );
        assert_eq!(
            config.rest_url.join(TCBS_TOKEN_PATH).unwrap().as_str(),
            "https://example.com/api/gaia/v1/oauth2/openapi/token"
        );
        assert_eq!(config.stream_url.as_str(), "ws://localhost:9000/stream");
        assert!(!format!("{config:?}").contains("123456"));
    }

    #[test]
    fn test_tcbs_config_missing_credentials() {
        for vars in [
            vec![],
            vec![(ENV_TCBS_API_KEY, "key")],
            vec![(ENV_TCBS_TOKEN, " ")],
        ] {
            assert!(matches!(
                TcbsConfig::from_lookup(lookup(&vars), stream_url()),
                Err(SocketError::Subscribe(msg)) if msg.contains(ENV_TCBS_TOKEN)
            ));
        }
    }

    #[test]
    fn test_parse_token_response() {
        assert_eq!(
            parse_token_response(r#"{"token":"eyJ.abc.def"}"#.to_string()).unwrap(),
            "eyJ.abc.def"
        );
        assert!(matches!(
            parse_token_response(r#"{"token":""}"#.to_string()),
            Err(SocketError::Subscribe(_))
        ));
        assert!(matches!(
            parse_token_response("nope".to_string()),
            Err(SocketError::Deserialise { .. })
        ));
    }

    #[test]
    fn test_jwt_expiry() {
        let claims =
            base64::engine::general_purpose::URL_SAFE_NO_PAD.encode(r#"{"exp":1763200000}"#);
        let token = format!("header.{claims}.signature");
        assert_eq!(
            jwt_expiry(&token),
            DateTime::from_timestamp(1_763_200_000, 0)
        );
        assert_eq!(jwt_expiry("opaque-token"), None);
    }

    #[test]
    fn test_cached_token_usable() {
        let now = Utc::now();
        let cached = CachedToken {
            api_key: "key".to_string(),
            token: "jwt".to_string(),
            expires_at: now + TimeDelta::hours(1),
        };
        assert!(cached.is_usable_for("key", now));
        assert!(!cached.is_usable_for("other", now));
        assert!(!cached.is_usable_for("key", now + TimeDelta::minutes(56)));
    }

    #[tokio::test]
    async fn test_access_token_passes_through_static_token() {
        let config = TcbsConfig {
            credentials: TcbsCredentials::Token("jwt".to_string()),
            rest_url: Url::parse(BASE_URL_TCBS_REST).unwrap(),
            stream_url: stream_url(),
        };
        assert_eq!(access_token(&config).await.unwrap(), "jwt");
    }
}
