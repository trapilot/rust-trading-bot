use super::message::{
    TCBS_CONTROL_AUTH, TCBS_CONTROL_SUBSCRIBED, TcbsAuthResponse, TcbsFrame, parse_text,
};
use crate::{
    exchange::Connector,
    subscriber::validator::SubscriptionValidator,
    subscription::{Map, SubscriptionKind},
};
use async_trait::async_trait;
use futures::StreamExt;
use serde::{Deserialize, Serialize};
use tracing::debug;
use trading_bot_integration::{
    Validator,
    error::SocketError,
    protocol::websocket::{WebSocket, WsMessage},
};

/// [`SubscriptionValidator`] for [`Tcbs`](super::Tcbs).
///
/// Counts `d|34|..` subscription acknowledgements until [`Connector::expected_responses`] is
/// reached, failing on a rejected `d|0|{"success":false}` response, a close frame or a timeout.
/// Data frames (`s|..`) received meanwhile are buffered for the transformer.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct TcbsSubValidator;

#[async_trait]
impl SubscriptionValidator for TcbsSubValidator {
    type Parser = super::message::TcbsParser;

    async fn validate<Exchange, InstrumentKey, Kind>(
        instrument_map: Map<InstrumentKey>,
        websocket: &mut WebSocket,
    ) -> Result<(Map<InstrumentKey>, Vec<WsMessage>), SocketError>
    where
        Exchange: Connector + Send,
        InstrumentKey: Send,
        Kind: SubscriptionKind + Send,
    {
        let timeout = Exchange::subscription_timeout();
        let expected_responses = Exchange::expected_responses(&instrument_map);
        let mut success_responses = 0usize;
        let mut buffered = Vec::new();

        let validation = async {
            while success_responses < expected_responses {
                let message = match websocket.next().await {
                    Some(Ok(message)) => message,
                    Some(Err(error)) => return Err(SocketError::WebSocket(Box::new(error))),
                    None => {
                        return Err(SocketError::Subscribe(
                            "WebSocket stream terminated unexpectedly".to_string(),
                        ));
                    }
                };

                match &message {
                    WsMessage::Text(text) => match parse_text(text.as_str()) {
                        Ok(TcbsFrame::Control(control))
                            if control.code == TCBS_CONTROL_SUBSCRIBED =>
                        {
                            success_responses += 1;
                            debug!(
                                exchange = %Exchange::ID,
                                %success_responses,
                                %expected_responses,
                                payload = %control.payload,
                                "received TCBS subscription acknowledgement",
                            );
                        }
                        Ok(TcbsFrame::Control(control)) if control.code == TCBS_CONTROL_AUTH => {
                            if let Ok(response) =
                                serde_json::from_str::<TcbsAuthResponse>(&control.payload)
                            {
                                response.validate()?;
                            }
                        }
                        Ok(TcbsFrame::Data(_)) => buffered.push(message),
                        // Other control frames (timeout config, heartbeat) or unparsable frames
                        _ => {}
                    },
                    WsMessage::Close(close_frame) => {
                        return Err(SocketError::Subscribe(format!(
                            "received WebSocket CloseFrame: {close_frame:?}"
                        )));
                    }
                    _ => {}
                }
            }
            Ok(())
        };

        match tokio::time::timeout(timeout, validation).await {
            Ok(Ok(())) => {
                debug!(exchange = %Exchange::ID, "validated exchange WebSocket subscriptions");
                Ok((instrument_map, buffered))
            }
            Ok(Err(error)) => Err(error),
            Err(_) => Err(SocketError::Subscribe(format!(
                "subscription validation timeout reached: {timeout:?}"
            ))),
        }
    }
}
