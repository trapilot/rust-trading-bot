use super::{
    config::{TcbsConfig, access_token, invalidate_cached_token},
    message::{TCBS_CONTROL_AUTH, TcbsAuthResponse, TcbsFrame, auth_frame, parse_text},
};
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
use futures::{SinkExt, StreamExt};
use serde::{Deserialize, Serialize};
use std::time::Duration;
use tracing::debug;
use trading_bot_integration::{
    Validator,
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage, connect},
};

/// [`Subscriber`] for [`Tcbs`](super::Tcbs).
///
/// 1. Resolves the Open API JWT (see [`access_token`]).
/// 2. Connects to the price board WebSocket.
/// 3. Sends `d|a|||<base64 token>` and waits for `d|0|{"success":true}` (TCBS rejects subscribe
///    frames sent before authentication completes).
/// 4. Sends the `d|s|tk|..` subscription and validates the `d|34|..` acknowledgement.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct TcbsSubscriber;

#[async_trait]
impl Subscriber for TcbsSubscriber {
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
        let config = TcbsConfig::from_env(Exchange::url()?)?;
        debug!(%exchange, ?config, ?subscriptions, "subscribing to TCBS price board");

        let token = access_token(&config).await?;

        // Connect to exchange
        let mut websocket = connect(config.stream_url.clone()).await?;
        debug!(%exchange, ?subscriptions, "connected to TCBS WebSocket");

        // Authenticate before subscribing
        if let Err(error) =
            authenticate(&mut websocket, &token, Exchange::subscription_timeout()).await
        {
            if matches!(error, SocketError::Subscribe(_)) {
                // Server rejected the token: don't reuse a cached one on the next attempt
                invalidate_cached_token().await;
            }
            return Err(error);
        }
        debug!(%exchange, "authenticated TCBS WebSocket");

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

        debug!(%exchange, "successfully initialised TCBS stream with confirmed Subscriptions");
        Ok(Subscribed {
            websocket,
            map,
            buffered_websocket_events,
        })
    }
}

/// Send the auth frame and wait for the `d|0|{...}` response.
pub async fn authenticate(
    websocket: &mut WebSocket,
    token: &str,
    timeout: Duration,
) -> Result<(), SocketError> {
    websocket
        .send(auth_frame(token))
        .await
        .map_err(|error| SocketError::WebSocket(Box::new(error)))?;

    let wait_for_response = async {
        loop {
            let message = match websocket.next().await {
                Some(Ok(message)) => message,
                Some(Err(error)) => return Err(SocketError::WebSocket(Box::new(error))),
                None => {
                    return Err(SocketError::Subscribe(
                        "TCBS WebSocket closed during authentication".to_string(),
                    ));
                }
            };

            match message {
                WsMessage::Text(text) => {
                    if let Ok(TcbsFrame::Control(control)) = parse_text(text.as_str())
                        && control.code == TCBS_CONTROL_AUTH
                    {
                        return serde_json::from_str::<TcbsAuthResponse>(&control.payload)
                            .map_err(|error| SocketError::Deserialise {
                                error,
                                payload: control.payload.clone(),
                            })?
                            .validate()
                            .map(|_| ());
                    }
                    // eg/ d|33|15 timeout config: keep waiting
                }
                WsMessage::Close(close_frame) => {
                    return Err(SocketError::Subscribe(format!(
                        "TCBS WebSocket closed during authentication: {close_frame:?}"
                    )));
                }
                _ => {}
            }
        }
    };

    tokio::time::timeout(timeout, wait_for_response)
        .await
        .map_err(|_| {
            SocketError::Subscribe(format!(
                "TCBS authentication response not received within {timeout:?}"
            ))
        })?
}
