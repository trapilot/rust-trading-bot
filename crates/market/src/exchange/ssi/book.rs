use super::{
    channel::SsiChannel,
    market::SsiMarket,
    message::{ssi_datetime_utc, value_to_decimal},
    transformer::SsiPayload,
};
use crate::{
    Identifier,
    books::{Level, OrderBook},
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::book::{OrderBookEvent, OrderBookL1},
};
use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol_str::SmolStr;
use trading_bot_instrument::exchange::ExchangeId;
use trading_bot_integration::subscription::SubscriptionId;

/// Number of price levels per side in an SSI `X-QUOTE` message.
pub const SSI_QUOTE_DEPTH: usize = 10;

/// [`Ssi`](super::Ssi) real-time quote (`X-QUOTE` channel) message content, holding the top
/// [`SSI_QUOTE_DEPTH`] bid & ask levels.
///
/// Every message is a full snapshot of those levels, so it maps to an [`OrderBookL1`] or an
/// [`OrderBookEvent::Snapshot`]. Empty levels (price or volume of 0, or non-numeric prices
/// such as "ATO"/"ATC") are dropped.
///
/// ### Raw Payload Examples
/// ```json
/// {
///   "TradingDate":"15/11/2026","Time":"09:15:03","Exchange":"HOSE","Symbol":"VNM",
///   "RType":"Quote","TradingSession":"LO",
///   "AskPrice1":65600.0,"AskVol1":1200.0, "AskPrice2":65700.0,"AskVol2":3400.0, ...
///   "BidPrice1":65500.0,"BidVol1":800.0,  "BidPrice2":65400.0,"BidVol2":5100.0, ...
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Serialize)]
pub struct SsiQuote {
    pub symbol: SmolStr,
    pub time: Option<DateTime<Utc>>,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

impl<'de> Deserialize<'de> for SsiQuote {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let fields = Map::<String, Value>::deserialize(deserializer)?;

        let symbol = fields
            .get("Symbol")
            .and_then(Value::as_str)
            .map(SmolStr::new)
            .ok_or_else(|| serde::de::Error::missing_field("Symbol"))?;

        let time = match (
            fields.get("TradingDate").and_then(Value::as_str),
            fields.get("Time").and_then(Value::as_str),
        ) {
            (Some(date), Some(time)) => ssi_datetime_utc(date, time),
            _ => None,
        };

        Ok(Self {
            symbol,
            time,
            bids: levels(&fields, "BidPrice", "BidVol"),
            asks: levels(&fields, "AskPrice", "AskVol"),
        })
    }
}

fn levels(fields: &Map<String, Value>, price_prefix: &str, volume_prefix: &str) -> Vec<Level> {
    (1..=SSI_QUOTE_DEPTH)
        .filter_map(|depth| {
            let price = value_to_decimal(fields.get(&format!("{price_prefix}{depth}"))?)?;
            let amount = value_to_decimal(fields.get(&format!("{volume_prefix}{depth}"))?)?;
            (price.is_sign_positive() && !price.is_zero() && !amount.is_zero())
                .then(|| Level::new(price, amount))
        })
        .collect()
}

impl SsiQuote {
    /// Highest bid level, if any.
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.iter().max_by_key(|level| level.price).copied()
    }

    /// Lowest ask level, if any.
    pub fn best_ask(&self) -> Option<Level> {
        self.asks.iter().min_by_key(|level| level.price).copied()
    }
}

impl SsiPayload for SsiQuote {
    fn subscription_id(&self) -> SubscriptionId {
        ExchangeSub::from((SsiChannel::QUOTES, SsiMarket::from(self.symbol.as_str()))).id()
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, SsiQuote)>
    for MarketIter<InstrumentKey, OrderBookL1>
{
    fn from((exchange_id, instrument, quote): (ExchangeId, InstrumentKey, SsiQuote)) -> Self {
        let time_received = Utc::now();
        let time_exchange = quote.time.unwrap_or(time_received);

        Self(vec![Ok(MarketEvent {
            time_exchange,
            time_received,
            exchange: exchange_id,
            instrument,
            kind: OrderBookL1 {
                last_update_time: time_exchange,
                best_bid: quote.best_bid(),
                best_ask: quote.best_ask(),
            },
        })])
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, SsiQuote)>
    for MarketIter<InstrumentKey, OrderBookEvent>
{
    fn from((exchange_id, instrument, quote): (ExchangeId, InstrumentKey, SsiQuote)) -> Self {
        let time_received = Utc::now();
        let time_exchange = quote.time.unwrap_or(time_received);

        Self(vec![Ok(MarketEvent {
            time_exchange,
            time_received,
            exchange: exchange_id,
            instrument,
            kind: OrderBookEvent::Snapshot(OrderBook::new(
                0,
                Some(time_exchange),
                quote.bids,
                quote.asks,
            )),
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use chrono::NaiveDate;
    use rust_decimal_macros::dec;

    const QUOTE: &str = r#"{
        "TradingDate":"15/11/2026","Time":"09:15:03","Exchange":"HOSE","Symbol":"VNM",
        "RType":"Quote","TradingSession":"LO",
        "AskPrice1":65600.0,"AskVol1":1200.0,"AskPrice2":65700.0,"AskVol2":3400.0,
        "AskPrice3":0.0,"AskVol3":0.0,
        "BidPrice1":65500.0,"BidVol1":800.0,"BidPrice2":"65400","BidVol2":5100,
        "BidPrice3":"ATO","BidVol3":100.0,"BidPrice4":null,"BidVol4":null
    }"#;

    fn time() -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(2026, 11, 15)
            .unwrap()
            .and_hms_opt(2, 15, 3)
            .unwrap()
            .and_utc()
    }

    #[test]
    fn test_de_ssi_quote() {
        let quote = serde_json::from_str::<SsiQuote>(QUOTE).unwrap();
        assert_eq!(
            quote,
            SsiQuote {
                symbol: SmolStr::new("VNM"),
                time: Some(time()),
                bids: vec![
                    Level::new(dec!(65500), dec!(800)),
                    Level::new(dec!(65400), dec!(5100)),
                ],
                asks: vec![
                    Level::new(dec!(65600), dec!(1200)),
                    Level::new(dec!(65700), dec!(3400)),
                ],
            }
        );
        assert_eq!(quote.subscription_id(), SubscriptionId::from("X-QUOTE|VNM"));
    }

    #[test]
    fn test_de_ssi_quote_missing_symbol() {
        assert!(serde_json::from_str::<SsiQuote>(r#"{"BidPrice1":1}"#).is_err());
    }

    #[test]
    fn test_ssi_quote_to_l1() {
        let quote = serde_json::from_str::<SsiQuote>(QUOTE).unwrap();
        let event = MarketIter::<u8, OrderBookL1>::from((ExchangeId::Ssi, 1, quote))
            .0
            .remove(0)
            .unwrap();

        assert_eq!(event.time_exchange, time());
        assert_eq!(
            event.kind,
            OrderBookL1 {
                last_update_time: time(),
                best_bid: Some(Level::new(dec!(65500), dec!(800))),
                best_ask: Some(Level::new(dec!(65600), dec!(1200))),
            }
        );
    }

    #[test]
    fn test_ssi_quote_to_l2_snapshot() {
        let quote = serde_json::from_str::<SsiQuote>(QUOTE).unwrap();
        let event = MarketIter::<u8, OrderBookEvent>::from((ExchangeId::Ssi, 1, quote))
            .0
            .remove(0)
            .unwrap();

        let OrderBookEvent::Snapshot(book) = event.kind else {
            panic!("expected OrderBookEvent::Snapshot");
        };
        assert_eq!(book.time_engine(), Some(time()));
        assert_eq!(
            book.bids().levels(),
            &[
                Level::new(dec!(65500), dec!(800)),
                Level::new(dec!(65400), dec!(5100))
            ]
        );
        assert_eq!(
            book.asks().levels(),
            &[
                Level::new(dec!(65600), dec!(1200)),
                Level::new(dec!(65700), dec!(3400))
            ]
        );
    }
}
