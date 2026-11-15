use super::{SSI_SIGNALR_HUB, config::SsiConfig};
use crate::{
    Identifier,
    exchange::Connector,
    instrument::InstrumentData,
    subscriber::{
        Subscribed, Subscriber,
        mapper::{SubscriptionMapper, WebSocketSubMapper},
        validator::SubscriptionValidator,
    },
    subscription::{Subscription, SubscriptionKind, SubscriptionMeta},
};
use async_trait::async_trait;
use futures::SinkExt;
use serde::{Deserialize, Serialize};
use serde_json::json;
use std::time::Duration;
use tokio_tungstenite::tungstenite::{
    client::IntoClientRequest,
    http::{HeaderValue, Request, header::AUTHORIZATION},
};
use tracing::debug;
use trading_bot_integration::{error::SocketError, protocol::websocket::connect};
use url::Url;

/// Path (relative to the access token REST base url) of the SSI access token endpoint.
pub const SSI_ACCESS_TOKEN_PATH: &str = "api/v2/Market/AccessToken";

/// Path (relative to the streaming base url) of the SSI SignalR endpoint.
pub const SSI_SIGNALR_PATH: &str = "v2.0/signalr/";

/// SignalR client protocol version spoken by SSI.
pub const SSI_SIGNALR_CLIENT_PROTOCOL: &str = "1.5";

/// Timeout for the SSI access token & SignalR negotiate HTTP requests.
const SSI_HTTP_TIMEOUT: Duration = Duration::from_secs(15);

/// [`Subscriber`] for [`Ssi`](super::Ssi).
///
/// SSI streams over a classic ASP.NET SignalR hub, so before the WebSocket can be opened it:
/// 1. Fetches a JWT access token with the configured consumer credentials.
/// 2. Negotiates a SignalR `ConnectionToken` using that JWT.
/// 3. Connects to the SignalR WebSocket (with the JWT in the `Authorization` header).
///
/// It then sends the `SwitchChannels` subscription and validates the invocation result exactly
/// like the standard [`WebSocketSubscriber`](crate::subscriber::WebSocketSubscriber).
///
/// A fresh token is fetched for every (re)connection.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct SsiSubscriber;

#[async_trait]
impl Subscriber for SsiSubscriber {
    type SubMapper = WebSocketSubMapper;

    async fn subscribe<Exchange, Instrument, Kind>(
        subscriptions: &[Subscription<Exchange, Instrument, Kind>],
    ) -> Result<Subscribed<Instrument::Key>, SocketError>
    where
        Exchange: Connector + Send + Sync,
        Kind: SubscriptionKind + Send + Sync,
        Instrument: InstrumentData,
        Subscription<Exchange, Instrument, Kind>:
            Identifier<Exchange::Channel> + Identifier<Exchange::Market>,
    {
        let exchange = Exchange::ID;
        let config = SsiConfig::from_env(Exchange::url()?)?;
        debug!(%exchange, ?config, ?subscriptions, "subscribing to SSI SignalR hub");

        let http = reqwest::Client::builder()
            .timeout(SSI_HTTP_TIMEOUT)
            .build()
            .map_err(SocketError::Http)?;

        // Authenticate & negotiate SignalR connection
        let token = fetch_access_token(&http, &config).await?;
        let negotiation = negotiate(&http, &config.stream_url, &token).await?;
        let request = websocket_request(&config.stream_url, &negotiation, &token)?;

        // Connect to exchange
        let mut websocket = connect(request).await?;
        debug!(%exchange, ?subscriptions, "connected to SSI SignalR WebSocket");

        // Map &[Subscription<Exchange, Kind>] to SubscriptionMeta
        let SubscriptionMeta {
            instrument_map,
            ws_subscriptions,
        } = Self::SubMapper::map::<Exchange, Instrument, Kind>(subscriptions);

        // Send Subscriptions over WebSocket
        for subscription in ws_subscriptions {
            debug!(%exchange, payload = ?subscription, "sending exchange subscription");
            websocket
                .send(subscription)
                .await
                .map_err(|error| SocketError::WebSocket(Box::new(error)))?;
        }

        // Validate Subscription responses
        let (map, buffered_websocket_events) = Exchange::SubValidator::validate::<
            Exchange,
            Instrument::Key,
            Kind,
        >(instrument_map, &mut websocket)
        .await?;

        debug!(%exchange, "successfully initialised SSI stream with confirmed Subscriptions");
        Ok(Subscribed {
            websocket,
            map,
            buffered_websocket_events,
        })
    }
}

/// SSI access token REST response.
///
/// ```json
/// {"message":"Success","status":200,"data":{"accessToken":"eyJhbGciOi..."}}
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize)]
struct SsiAccessTokenResponse {
    #[serde(default)]
    message: Option<String>,
    #[serde(default)]
    data: Option<SsiAccessTokenData>,
}

#[derive(Clone, PartialEq, Debug, Deserialize)]
struct SsiAccessTokenData {
    #[serde(rename = "accessToken", default)]
    access_token: Option<String>,
}

/// SignalR negotiate response (only the fields required to connect).
///
/// ```json
/// {"Url":"/v2.0/signalr","ConnectionToken":"AQAAANCMnd8B...","ConnectionId":"...","ProtocolVersion":"1.5", ...}
/// ```
#[derive(Clone, PartialEq, Eq, Debug, Deserialize)]
pub struct SignalRNegotiation {
    #[serde(rename = "ConnectionToken")]
    pub connection_token: String,
    #[serde(rename = "ProtocolVersion", default)]
    pub protocol_version: Option<String>,
}

async fn fetch_access_token(
    http: &reqwest::Client,
    config: &SsiConfig,
) -> Result<String, SocketError> {
    let url = config.data_url.join(SSI_ACCESS_TOKEN_PATH)?;
    let body = json!({
        "consumerID": config.consumer_id,
        "consumerSecret": config.consumer_secret,
    });

    let response = http
        .post(url)
        .json(&body)
        .send()
        .await
        .map_err(SocketError::Http)?;
    let status = response.status();
    let payload = response.text().await.map_err(SocketError::Http)?;
    if !status.is_success() {
        return Err(SocketError::HttpResponse(status, payload));
    }

    parse_access_token(payload)
}

fn parse_access_token(payload: String) -> Result<String, SocketError> {
    let response = serde_json::from_str::<SsiAccessTokenResponse>(&payload)
        .map_err(|error| SocketError::Deserialise { error, payload })?;

    response
        .data
        .and_then(|data| data.access_token)
        .filter(|token| !token.is_empty())
        .ok_or_else(|| {
            SocketError::Subscribe(format!(
                "SSI access token request failed: {}",
                response.message.unwrap_or_default()
            ))
        })
}

/// SignalR `connectionData` query parameter naming the hub to join.
fn connection_data() -> String {
    json!([{ "name": SSI_SIGNALR_HUB }]).to_string()
}

fn bearer(token: &str) -> Result<HeaderValue, SocketError> {
    let mut value = HeaderValue::from_str(&format!("Bearer {token}"))
        .map_err(|error| SocketError::Subscribe(format!("invalid SSI access token: {error}")))?;
    // Keep the token out of Debug output (eg/ connection logs)
    value.set_sensitive(true);
    Ok(value)
}

fn negotiate_url(stream_url: &Url) -> Result<Url, SocketError> {
    let mut url = stream_url.join(SSI_SIGNALR_PATH)?.join("negotiate")?;
    url.query_pairs_mut()
        .append_pair("connectionData", &connection_data())
        .append_pair("clientProtocol", SSI_SIGNALR_CLIENT_PROTOCOL);
    Ok(url)
}

async fn negotiate(
    http: &reqwest::Client,
    stream_url: &Url,
    token: &str,
) -> Result<SignalRNegotiation, SocketError> {
    let response = http
        .post(negotiate_url(stream_url)?)
        .header(AUTHORIZATION.as_str(), bearer(token)?.as_bytes())
        .send()
        .await
        .map_err(SocketError::Http)?;
    let status = response.status();
    let payload = response.text().await.map_err(SocketError::Http)?;
    if !status.is_success() {
        return Err(SocketError::HttpResponse(status, payload));
    }

    serde_json::from_str(&payload).map_err(|error| SocketError::Deserialise { error, payload })
}

/// Build the SignalR WebSocket connect [`Url`] from the negotiation result.
pub fn connect_url(stream_url: &Url, negotiation: &SignalRNegotiation) -> Result<Url, SocketError> {
    let mut url = stream_url.join(SSI_SIGNALR_PATH)?.join("connect")?;

    let scheme = match url.scheme() {
        "https" | "wss" => "wss",
        _ => "ws",
    };
    url.set_scheme(scheme).map_err(|()| {
        SocketError::Subscribe(format!("cannot convert SSI stream url to WebSocket: {url}"))
    })?;

    url.query_pairs_mut()
        .append_pair("transport", "webSockets")
        .append_pair("connectionToken", &negotiation.connection_token)
        .append_pair("connectionData", &connection_data())
        .append_pair(
            "clientProtocol",
            negotiation
                .protocol_version
                .as_deref()
                .unwrap_or(SSI_SIGNALR_CLIENT_PROTOCOL),
        );
    Ok(url)
}

fn websocket_request(
    stream_url: &Url,
    negotiation: &SignalRNegotiation,
    token: &str,
) -> Result<Request<()>, SocketError> {
    let mut request = connect_url(stream_url, negotiation)?
        .as_str()
        .into_client_request()
        .map_err(|error| SocketError::WebSocket(Box::new(error)))?;
    request.headers_mut().insert(AUTHORIZATION, bearer(token)?);
    Ok(request)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::exchange::ssi::BASE_URL_SSI_STREAM;

    fn query(url: &Url, key: &str) -> Option<String> {
        url.query_pairs()
            .find(|(k, _)| k == key)
            .map(|(_, v)| v.into_owned())
    }

    #[test]
    fn test_parse_access_token() {
        assert_eq!(
            parse_access_token(
                r#"{"message":"Success","status":200,"data":{"accessToken":"abc.def.ghi"}}"#
                    .to_string()
            )
            .unwrap(),
            "abc.def.ghi"
        );

        let error = parse_access_token(
            r#"{"message":"Invalid consumer","status":400,"data":null}"#.to_string(),
        )
        .unwrap_err();
        assert!(matches!(error, SocketError::Subscribe(msg) if msg.contains("Invalid consumer")));

        assert!(matches!(
            parse_access_token("<html>".to_string()),
            Err(SocketError::Deserialise { .. })
        ));
    }

    #[test]
    fn test_negotiate_url() {
        let url = negotiate_url(&Url::parse(BASE_URL_SSI_STREAM).unwrap()).unwrap();
        assert_eq!(url.scheme(), "https");
        assert_eq!(url.host_str(), Some("fc-datahub.ssi.com.vn"));
        assert_eq!(url.path(), "/v2.0/signalr/negotiate");
        assert_eq!(
            query(&url, "connectionData").as_deref(),
            Some(r#"[{"name":"FcMarketDataV2Hub"}]"#)
        );
        assert_eq!(query(&url, "clientProtocol").as_deref(), Some("1.5"));
    }

    #[test]
    fn test_connect_url() {
        let negotiation = serde_json::from_str::<SignalRNegotiation>(
            r#"{"Url":"/v2.0/signalr","ConnectionToken":"tok/en+=","ConnectionId":"x","ProtocolVersion":"1.5"}"#,
        )
        .unwrap();

        let url = connect_url(&Url::parse(BASE_URL_SSI_STREAM).unwrap(), &negotiation).unwrap();
        assert_eq!(url.scheme(), "wss");
        assert_eq!(url.path(), "/v2.0/signalr/connect");
        assert_eq!(query(&url, "transport").as_deref(), Some("webSockets"));
        assert_eq!(query(&url, "connectionToken").as_deref(), Some("tok/en+="));
        assert_eq!(query(&url, "clientProtocol").as_deref(), Some("1.5"));

        let url =
            connect_url(&Url::parse("http://localhost:8080/").unwrap(), &negotiation).unwrap();
        assert_eq!(url.scheme(), "ws");
    }

    #[test]
    fn test_websocket_request_has_redacted_bearer() {
        let negotiation = SignalRNegotiation {
            connection_token: "token".to_string(),
            protocol_version: None,
        };
        let request = websocket_request(
            &Url::parse(BASE_URL_SSI_STREAM).unwrap(),
            &negotiation,
            "secret-jwt",
        )
        .unwrap();

        let auth = request.headers().get(AUTHORIZATION).unwrap();
        assert_eq!(auth.to_str().unwrap(), "Bearer secret-jwt");
        assert!(auth.is_sensitive());
        assert!(!format!("{request:?}").contains("secret-jwt"));
    }
}
