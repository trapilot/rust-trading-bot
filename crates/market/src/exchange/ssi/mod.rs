//! [`Ssi`] (SSI Securities, Vietnam stock market) integration via SSI FastConnect Data.
//!
//! SSI streams market data over a classic ASP.NET SignalR hub (`FcMarketDataV2Hub`) rather than
//! a plain WebSocket API, so connecting takes three steps (see [`subscriber::SsiSubscriber`]):
//! 1. `POST {data_url}/api/v2/Market/AccessToken` with the consumer credentials -> JWT.
//! 2. `POST {stream_url}/v2.0/signalr/negotiate` with the JWT -> SignalR `ConnectionToken`.
//! 3. Open the WebSocket at `{stream_url}/v2.0/signalr/connect` and invoke `SwitchChannels`.
//!
//! Credentials are read from the environment, see [`config::SsiConfig`].
//!
//! Supported [`SubscriptionKind`](crate::subscription::SubscriptionKind)s:
//! - [`PublicTrades`] (`X-TRADE` channel)
//! - [`OrderBooksL1`] (`X-QUOTE` channel, best bid/ask)
//! - [`OrderBooksL2`] (`X-QUOTE` channel, 10 levels; every message is a full snapshot)
//!
//! Instruments are Vietnamese equities, eg/ `(Ssi, "vnm", "vnd", MarketDataInstrumentKind::Spot, PublicTrades)`.
//! The SSI symbol is the uppercased base asset (eg/ "VNM").

use self::{
    book::SsiQuote, channel::SsiChannel, market::SsiMarket, subscriber::SsiSubscriber,
    subscription::SsiSubResponse, trade::SsiTrade, transformer::SsiTransformer,
};
use crate::{
    ExchangeWsStream, NoInitialSnapshots,
    exchange::{Connector, ExchangeSub, StreamSelector},
    instrument::InstrumentData,
    subscriber::validator::WebSocketSubValidator,
    subscription::{
        Map,
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
};
use derive_more::Display;
use itertools::Itertools;
use serde_json::json;
use trading_bot_instrument::exchange::ExchangeId;
use trading_bot_integration::protocol::websocket::{WebSocketSerdeParser, WsMessage};
use trading_bot_macro::{DeExchange, SerExchange};
use url::Url;

/// Order book (`X-QUOTE`) message types for [`Ssi`].
pub mod book;

/// Defines the type that translates a Trading Bot [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific channel used for generating [`Connector::requests`].
pub mod channel;

/// Runtime configuration (credentials & server urls) for [`Ssi`].
pub mod config;

/// Defines the type that translates a Trading Bot [`Subscription`](crate::subscription::Subscription)
/// into an exchange [`Connector`] specific market used for generating [`Connector::requests`].
pub mod market;

/// SignalR message envelope types for [`Ssi`].
pub mod message;

/// [`Subscriber`](crate::subscriber::Subscriber) that authenticates, negotiates and connects to
/// the [`Ssi`] SignalR hub.
pub mod subscriber;

/// [`Subscription`](crate::subscription::Subscription) response type and response
/// [`Validator`](trading_bot_integration::Validator) for [`Ssi`].
pub mod subscription;

/// Public trade (`X-TRADE`) message types for [`Ssi`].
pub mod trade;

/// [`ExchangeTransformer`](crate::transformer::ExchangeTransformer) for [`Ssi`] SignalR frames.
pub mod transformer;

/// Default [`Ssi`] FastConnect Data REST base url (used to fetch access tokens).
pub const BASE_URL_SSI_DATA: &str = "https://fc-data.ssi.com.vn/";

/// Default [`Ssi`] FastConnect Data streaming base url (SignalR hub host).
pub const BASE_URL_SSI_STREAM: &str = "https://fc-datahub.ssi.com.vn/";

/// [`Ssi`] SignalR hub name for market data.
pub const SSI_SIGNALR_HUB: &str = "FcMarketDataV2Hub";

/// [`Ssi`] SignalR hub method used to (re)select the subscribed channel.
pub const SSI_SIGNALR_SWITCH_CHANNELS: &str = "SwitchChannels";

/// Convenient type alias for an Ssi [`ExchangeWsStream`] using [`WebSocketSerdeParser`].
pub type SsiWsStream<Transformer> = ExchangeWsStream<WebSocketSerdeParser, Transformer>;

/// [`Ssi`] exchange (SSI Securities, Vietnam stock market).
///
/// See docs: <https://guide.ssi.com.vn/ssi-products/fastconnect-data>
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
pub struct Ssi;

impl Connector for Ssi {
    const ID: ExchangeId = ExchangeId::Ssi;
    type Channel = SsiChannel;
    type Market = SsiMarket;
    type Subscriber = SsiSubscriber;
    type SubValidator = WebSocketSubValidator;
    type SubResponse = SsiSubResponse;

    /// Default streaming base url. Override at runtime with the `SSI_STREAM_URL` env var.
    fn url() -> Result<Url, url::ParseError> {
        Url::parse(BASE_URL_SSI_STREAM)
    }

    /// SSI accepts a single channel per connection, selected with one `SwitchChannels`
    /// invocation whose argument is `"{channel}:{SYMBOL1}-{SYMBOL2}-..."`.
    fn requests(exchange_subs: Vec<ExchangeSub<Self::Channel, Self::Market>>) -> Vec<WsMessage> {
        let Some(channel) = exchange_subs.first().map(|sub| sub.channel) else {
            return vec![];
        };

        let symbols = exchange_subs
            .iter()
            .map(|sub| sub.market.as_ref())
            .unique()
            .join("-");

        vec![WsMessage::text(
            json!({
                "H": SSI_SIGNALR_HUB,
                "M": SSI_SIGNALR_SWITCH_CHANNELS,
                "A": [format!("{}:{symbols}", channel.as_ref())],
                "I": 0,
            })
            .to_string(),
        )]
    }

    /// One `SwitchChannels` invocation is sent per connection, so one response is expected.
    fn expected_responses<InstrumentKey>(_: &Map<InstrumentKey>) -> usize {
        1
    }
}

impl<Instrument> StreamSelector<Instrument, PublicTrades> for Ssi
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = SsiWsStream<SsiTransformer<Instrument::Key, PublicTrades, SsiTrade>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL1> for Ssi
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = SsiWsStream<SsiTransformer<Instrument::Key, OrderBooksL1, SsiQuote>>;
}

impl<Instrument> StreamSelector<Instrument, OrderBooksL2> for Ssi
where
    Instrument: InstrumentData,
{
    type SnapFetcher = NoInitialSnapshots;
    type Stream = SsiWsStream<SsiTransformer<Instrument::Key, OrderBooksL2, SsiQuote>>;
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ssi_requests_single_switch_channels() {
        let subs = vec![
            ExchangeSub::from((SsiChannel::TRADES, SsiMarket::from("VNM"))),
            ExchangeSub::from((SsiChannel::TRADES, SsiMarket::from("HPG"))),
            ExchangeSub::from((SsiChannel::TRADES, SsiMarket::from("VNM"))),
        ];

        let requests = Ssi::requests(subs);
        assert_eq!(requests.len(), 1);

        let payload: serde_json::Value =
            serde_json::from_str(requests[0].to_text().unwrap()).unwrap();
        assert_eq!(
            payload,
            json!({
                "H": "FcMarketDataV2Hub",
                "M": "SwitchChannels",
                "A": ["X-TRADE:VNM-HPG"],
                "I": 0,
            })
        );
    }

    #[test]
    fn test_ssi_requests_empty() {
        assert!(Ssi::requests(vec![]).is_empty());
    }
}
