use super::Ssi;
use crate::{
    Identifier,
    subscription::{
        Subscription,
        book::{OrderBooksL1, OrderBooksL2},
        trade::PublicTrades,
    },
};
use serde::Serialize;

/// Type that defines how to translate a Trading Bot [`Subscription`] into an
/// [`Ssi`] channel to be subscribed to.
///
/// See docs: <https://guide.ssi.com.vn/ssi-products/fastconnect-data/streaming-data>
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Serialize)]
pub struct SsiChannel(pub &'static str);

impl SsiChannel {
    /// [`Ssi`] real-time matched trades channel.
    pub const TRADES: Self = Self("X-TRADE");

    /// [`Ssi`] real-time quote channel (10 levels of bids & asks).
    pub const QUOTES: Self = Self("X-QUOTE");
}

impl<Instrument> Identifier<SsiChannel> for Subscription<Ssi, Instrument, PublicTrades> {
    fn id(&self) -> SsiChannel {
        SsiChannel::TRADES
    }
}

impl<Instrument> Identifier<SsiChannel> for Subscription<Ssi, Instrument, OrderBooksL1> {
    fn id(&self) -> SsiChannel {
        SsiChannel::QUOTES
    }
}

impl<Instrument> Identifier<SsiChannel> for Subscription<Ssi, Instrument, OrderBooksL2> {
    fn id(&self) -> SsiChannel {
        SsiChannel::QUOTES
    }
}

impl AsRef<str> for SsiChannel {
    fn as_ref(&self) -> &str {
        self.0
    }
}
