use super::BASE_URL_SSI_DATA;
use std::fmt::{Debug, Formatter};
use trading_bot_integration::error::SocketError;
use url::Url;

/// Env var holding the SSI FastConnect Data consumer id (required).
pub const ENV_SSI_CONSUMER_ID: &str = "SSI_CONSUMER_ID";

/// Env var holding the SSI FastConnect Data consumer secret (required).
pub const ENV_SSI_CONSUMER_SECRET: &str = "SSI_CONSUMER_SECRET";

/// Env var overriding the SSI REST base url (optional, defaults to [`BASE_URL_SSI_DATA`]).
pub const ENV_SSI_DATA_URL: &str = "SSI_DATA_URL";

/// Env var overriding the SSI streaming base url (optional, defaults to
/// [`BASE_URL_SSI_STREAM`](super::BASE_URL_SSI_STREAM)).
pub const ENV_SSI_STREAM_URL: &str = "SSI_STREAM_URL";

/// Runtime configuration for connecting to SSI FastConnect Data.
///
/// The [`Connector`](crate::exchange::Connector) interface is static (no instance state), so the
/// credentials are read from the environment each time a connection is (re)established.
#[derive(Clone)]
pub struct SsiConfig {
    pub consumer_id: String,
    pub consumer_secret: String,
    pub data_url: Url,
    pub stream_url: Url,
}

impl Debug for SsiConfig {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SsiConfig")
            .field("consumer_id", &self.consumer_id)
            .field("consumer_secret", &"<redacted>")
            .field("data_url", &self.data_url)
            .field("stream_url", &self.stream_url)
            .finish()
    }
}

impl SsiConfig {
    /// Build an [`SsiConfig`] from the `SSI_*` environment variables, using `default_stream_url`
    /// when `SSI_STREAM_URL` is unset.
    pub fn from_env(default_stream_url: Url) -> Result<Self, SocketError> {
        Self::from_lookup(|key| std::env::var(key).ok(), default_stream_url)
    }

    fn from_lookup<F>(lookup: F, default_stream_url: Url) -> Result<Self, SocketError>
    where
        F: Fn(&str) -> Option<String>,
    {
        let required = |key: &str| {
            lookup(key)
                .filter(|value| !value.is_empty())
                .ok_or_else(|| {
                    SocketError::Subscribe(format!("SSI requires the {key} environment variable"))
                })
        };

        let data_url = match lookup(ENV_SSI_DATA_URL) {
            Some(url) => parse_base_url(&url)?,
            None => Url::parse(BASE_URL_SSI_DATA)?,
        };
        let stream_url = match lookup(ENV_SSI_STREAM_URL) {
            Some(url) => parse_base_url(&url)?,
            None => default_stream_url,
        };

        Ok(Self {
            consumer_id: required(ENV_SSI_CONSUMER_ID)?,
            consumer_secret: required(ENV_SSI_CONSUMER_SECRET)?,
            data_url,
            stream_url,
        })
    }
}

/// Parse a base url, ensuring a trailing '/' so that [`Url::join`] appends rather than replaces
/// the last path segment.
fn parse_base_url(url: &str) -> Result<Url, SocketError> {
    let url = if url.ends_with('/') {
        Url::parse(url)
    } else {
        Url::parse(&format!("{url}/"))
    };
    url.map_err(SocketError::from)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::ssi::BASE_URL_SSI_STREAM;
    use std::collections::HashMap;

    fn lookup(vars: &[(&str, &str)]) -> impl Fn(&str) -> Option<String> {
        let vars: HashMap<String, String> = vars
            .iter()
            .map(|(k, v)| (k.to_string(), v.to_string()))
            .collect();
        move |key| vars.get(key).cloned()
    }

    #[test]
    fn test_ssi_config_defaults() {
        let config = SsiConfig::from_lookup(
            lookup(&[
                (ENV_SSI_CONSUMER_ID, "id"),
                (ENV_SSI_CONSUMER_SECRET, "secret"),
            ]),
            Url::parse(BASE_URL_SSI_STREAM).unwrap(),
        )
        .unwrap();

        assert_eq!(config.consumer_id, "id");
        assert_eq!(config.consumer_secret, "secret");
        assert_eq!(config.data_url.as_str(), BASE_URL_SSI_DATA);
        assert_eq!(config.stream_url.as_str(), BASE_URL_SSI_STREAM);
        assert!(!format!("{config:?}").contains("secret\""));
    }

    #[test]
    fn test_ssi_config_url_overrides_get_trailing_slash() {
        let config = SsiConfig::from_lookup(
            lookup(&[
                (ENV_SSI_CONSUMER_ID, "id"),
                (ENV_SSI_CONSUMER_SECRET, "secret"),
                (ENV_SSI_DATA_URL, "https://data.example.com/base"),
                (ENV_SSI_STREAM_URL, "https://stream.example.com"),
            ]),
            Url::parse(BASE_URL_SSI_STREAM).unwrap(),
        )
        .unwrap();

        assert_eq!(config.data_url.as_str(), "https://data.example.com/base/");
        assert_eq!(config.stream_url.as_str(), "https://stream.example.com/");
    }

    #[test]
    fn test_ssi_config_missing_credentials() {
        let result = SsiConfig::from_lookup(
            lookup(&[(ENV_SSI_CONSUMER_ID, "id")]),
            Url::parse(BASE_URL_SSI_STREAM).unwrap(),
        );
        assert!(
            matches!(result, Err(SocketError::Subscribe(msg)) if msg.contains(ENV_SSI_CONSUMER_SECRET))
        );
    }
}
