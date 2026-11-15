//! [`Tcbs`] (Techcombank Securities, Vietnam stock market) integration via the TCBS iFlash
//! Open API stock price board WebSocket.
//!
//! See docs: <https://developers.tcbs.com.vn/docs/v1.0.0/stock/ws-price-board/>
//!
//! The price board speaks a pipe-delimited text protocol rather than JSON:
//! - Client -> server: `d|a|||<base64 token>` (auth), `d|s|tk|<channels>|<SYMBOLS>` (subscribe),
//!   `d|p|||` (heartbeat, every 2s).
//! - Server -> client: `d|<code>|<payload>` control frames and `s|<channel>|<json>` data frames.
//!
//! Connecting therefore takes two steps (see [`subscriber::TcbsSubscriber`]):
//! 1. Obtain an Open API JWT (see [`config::TcbsConfig`]).
//! 2. Open the WebSocket, authenticate with the JWT, then subscribe.
//!
//! Supported [`SubscriptionKind`](crate::subscription::SubscriptionKind)s:
//! - [`PublicTrades`] (channel `s|6` ticker match). TCBS does not publish the aggressor side or a
//!   timestamp, see [`transformer`] for how they are derived.
//! - [`OrderBooksL1`] (channels `s|1` bids & `s|2` offers, best level).
//! - [`OrderBooksL2`] (channels `s|1` & `s|2`, top 3 levels, emitted as snapshots).
//!
//! Instruments are Vietnamese equities, eg/ `(Tcbs, "tcb", "vnd", MarketDataInstrumentKind::Spot, PublicTrades)`.
//! The TCBS symbol is the uppercased base asset (eg/ "TCB").

use self::{
    channel::TcbsChannel,
    market::TcbsMarket,
    message::{TCBS_HEARTBEAT, TcbsAuthResponse, TcbsParser},
    subscriber::TcbsSubscriber,
    transformer::TcbsTransformer,
    validator::TcbsSubValidator,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, PingInterval, StreamSelector},
    instrument::InstrumentData,
    subscription::{
        Map,
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
};
use derive_more::Display;
use itertools::Itertools;
use std::time::Duration;
use trading_bot_instrument::exchange::ExchangeId;
use trading_bot_integration::protocol::websocket::WsMessage;
use trading_bot_macro::{DeExchange, SerExchange};
use url::Url;

/// Defines the type that translates a Trading Bot [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Runtime configuration (credentials & server urls) and JWT handling for [`Tcbs`].
pub mod config;

/// Defines the type that translates a Trading Bot [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// Pipe-delimited wire protocol types & [`StreamParser`](trading_bot_integration::protocol::StreamParser)
/// for [`Tcbs`].
pub mod message;

/// [`Subscriber`](crate::subscriber::Subscriber) that authenticates over the WebSocket before
/// subscribing.
pub mod subscriber;

/// Stateful [`ExchangeTransformer`](crate::transformer::ExchangeTransformer) for [`Tcbs`].
pub mod transformer;

/// [`SubscriptionValidator`](crate::subscriber::validator::SubscriptionValidator) for [`Tcbs`]
/// subscription acknowledgements.
pub mod validator;

/// Default [`Tcbs`] Open API REST base url (used to exchange an API key for a JWT).
pub const BASE_URL_TCBS_REST: &str = "https://openapi.tcbs.com.vn/";

/// Default [`Tcbs`] stock price board WebSocket url.
pub const BASE_URL_TCBS_STREAM: &str = "wss://openapi.tcbs.com.vn/ws/thesis/v1/stream/normal";

/// Interval at which TCBS requires the client heartbeat text frame (server timeout is 15s).
pub const TCBS_HEARTBEAT_INTERVAL: Duration = Duration::from_secs(2);

/// Convenient type alias for a Tcbs [`ExchangeWsStream`] using the pipe-delimited [`TcbsParser`].
pub type TcbsWsStream<Transformer> = ExchangeWsStream<TcbsParser, Transformer>;

/// [`Tcbs`] exchange (Techcombank Securities, Vietnam stock market).
///
/// See docs: <https://developers.tcbs.com.vn/docs/v1.0.0/introduction/>
#[derive(
    Copy,
    Clone,
    Eq,
    PartialEq,
    Ord,
    PartialOrd,
    Hash,
    Debug,
    Default,
    Display,
    DeExchange,
    SerExchange,
)]
pub struct Tcbs;

impl Connector for Tcbs {
    const ID: ExchangeId = ExchangeId::Tcbs;
    type Channel = TcbsChannel;
    type Market = TcbsMarket;
    type Subscriber = TcbsSubscriber;
    type SubValidator = TcbsSubValidator;
    type SubResponse = TcbsAuthResponse;

    /// Default price board url. Override at runtime with the `TCBS_STREAM_URL` env var.
    fn url() -> Result<Url, url::ParseError> {
        Url::parse(BASE_URL_TCBS_STREAM)
    }

    /// TCBS only accepts its own `d|p|||` text heartbeat (RFC 6455 ping frames get the
    /// connection closed), sent every 2 seconds.
    fn ping_interval() -> Option<PingInterval> {
        Some(PingInterval {
            interval: tokio::time::interval(TCBS_HEARTBEAT_INTERVAL),
            ping: || WsMessage::text(TCBS_HEARTBEAT),
        })
    }

    /// One `d|s|tk|<channels>|<SYMBOL1>,<SYMBOL2>,...` subscribe frame per connection.
    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let Some(channel) = exchange_subs.first().map(|sub| sub.channel) else {
            return vec![];
        };

        let symbols = exchange_subs
            .iter()
            .map(|sub| sub.market.as_ref())
            .unique()
            .join(",");

        vec![WsMessage::text(format!(
            "d|s|tk|{}|{symbols}",
            channel.as_ref()
        ))]
    }

    /// One subscribe frame is sent per connection, so one `d|34|..` acknowledgement is expected.
    fn expected_responses<InstrumentKey>(_: &Map<InstrumentKey>) -> usize {
        1
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Tcbs
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = TcbsWsStream<TcbsTransformer<Instrument::Key, PublicTrades>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for Tcbs
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = TcbsWsStream<TcbsTransformer<Instrument::Key, OrderBooksL1>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for Tcbs
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = TcbsWsStream<TcbsTransformer<Instrument::Key, OrderBooksL2>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tcbs_requests_single_subscribe_frame() {
        let subs = vec![
            ExchangeSub::from((TcbsChannel::TRADES, TcbsMarket::from("tcb"))),
            ExchangeSub::from((TcbsChannel::TRADES, TcbsMarket::from("FPT"))),
            ExchangeSub::from((TcbsChannel::TRADES, TcbsMarket::from("TCB"))),
        ];

        let requests = Tcbs::requests(subs);
        assert_eq!(requests.len(), 1);
        assert_eq!(requests[0].to_text().unwrap(), "d|s|tk|tm+bi+op|TCB,FPT");
    }

    #[test]
    fn test_tcbs_requests_empty() {
        assert!(Tcbs::requests(vec![]).is_empty());
    }

    #[tokio::test]
    async fn test_tcbs_heartbeat() {
        let ping = Tcbs::ping_interval().unwrap();
        assert_eq!((ping.ping)().to_text().unwrap(), "d|p|||");
        assert_eq!(ping.interval.period(), TCBS_HEARTBEAT_INTERVAL);
    }
}
