use super::Tcbs;
use crate::{
    Identifier,
    subscription::{
        Subscription,
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
};
use serde::Serialize;

/// Type that defines how to translate a Trading Bot [`Subscription`] into the [`Tcbs`] price
/// board channel codes (the `<properties>` part of `d|s|tk|<properties>|<symbols>`).
///
/// | Code | Data                 | Server channel |
/// |------|----------------------|----------------|
/// | `bi` | Bids, top 3          | `s|1`          |
/// | `op` | Offers, top 3        | `s|2`          |
/// | `tm` | Ticker match (trade) | `s|6`          |
///
/// See docs: <https://developers.tcbs.com.vn/docs/v1.0.0/stock/ws-price-board/>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct TcbsChannel(pub &'static str);

impl TcbsChannel {
    /// Trades, plus bids & offers so the transformer can infer the aggressor side.
    pub const TRADES: Self = Self("tm+bi+op");

    /// Top 3 bids & offers.
    pub const QUOTES: Self = Self("bi+op");
}

impl<Instrument> Identifier<TcbsChannel> for Subscription<Tcbs, Instrument, PublicTrades> {
    fn id(&self) -> TcbsChannel {
        TcbsChannel::TRADES
    }
}

impl<Instrument> Identifier<TcbsChannel> for Subscription<Tcbs, Instrument, OrderBooksL1> {
    fn id(&self) -> TcbsChannel {
        TcbsChannel::QUOTES
    }
}

impl<Instrument> Identifier<TcbsChannel> for Subscription<Tcbs, Instrument, OrderBooksL2> {
    fn id(&self) -> TcbsChannel {
        TcbsChannel::QUOTES
    }
}

impl AsRef<str> for TcbsChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}
