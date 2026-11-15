use super::{
    channel::SsiChannel,
    market::SsiMarket,
    message::{de_flexible_f64, ssi_datetime_utc},
    transformer::{SsiPayload, SsiSequence},
};
use crate::{
    Identifier,
    event::{MarketEvent, MarketIter},
    exchange::ExchangeSub,
    subscription::trade::PublicTrade,
};
use chrono::{Datelike, NaiveDate, Utc};
use serde::{Deserialize, Serialize};
use smol_str::SmolStr;
use tracing::debug;
use trading_bot_instrument::{Side, exchange::ExchangeId};
use trading_bot_integration::subscription::SubscriptionId;

/// [`Ssi`](super::Ssi) real-time trade (`X-TRADE` channel) message content.
///
/// ### Raw Payload Examples
/// ```json
/// {
///   "RType":"Trade","TradingDate":"15/11/2026","Time":"09:15:03","Isin":"VN000000VNM1",
///   "Symbol":"VNM","Ceiling":69900.0,"Floor":60900.0,"RefPrice":65400.0,"AvgPrice":65512.3,
///   "PriorVal":65400.0,"LastPrice":65500.0,"LastVol":200.0,"TotalVal":1.3102e9,"TotalVol":20000.0,
///   "MarketId":"HOSE","Exchange":"HOSE","TradingSession":"LO","TradingStatus":"N",
///   "Change":100.0,"RatioChange":0.15,"EstMatchedPrice":0.0,"Highest":65600.0,"Lowest":65300.0,
///   "Side":"BU"
/// }
/// ```
///
/// SSI does not publish a trade id. `TotalVol` (cumulative matched volume for the day) is used
/// as the trade id & sequence instead: it strictly increases with every new match, which also
/// lets the transformer drop re-broadcasts of the same match.
#[derive(Clone, PartialEq, PartialOrd, Debug, Deserialize, Serialize)]
pub struct SsiTrade {
    #[serde(rename = "Symbol")]
    pub symbol: SmolStr,
    #[serde(rename = "TradingDate", default)]
    pub trading_date: String,
    #[serde(rename = "Time", default)]
    pub time: String,
    #[serde(rename = "LastPrice", deserialize_with = "de_flexible_f64")]
    pub last_price: f64,
    #[serde(rename = "LastVol", deserialize_with = "de_flexible_f64")]
    pub last_vol: f64,
    #[serde(rename = "TotalVol", default, deserialize_with = "de_flexible_f64")]
    pub total_vol: f64,
    #[serde(rename = "Side", default)]
    pub side: Option<String>,
}

impl SsiTrade {
    /// Aggressor side: SSI uses "BU" (buy-up, buyer initiated) and "SD" (sell-down, seller
    /// initiated). Auction matches (ATO/ATC) carry no side.
    pub fn aggressor_side(&self) -> Option<Side> {
        match self.side.as_deref().map(str::trim) {
            Some(side) if side.eq_ignore_ascii_case("BU") => Some(Side::Buy),
            Some(side) if side.eq_ignore_ascii_case("SD") => Some(Side::Sell),
            _ => None,
        }
    }
}

impl SsiPayload for SsiTrade {
    fn subscription_id(&self) -> SubscriptionId {
        ExchangeSub::from((SsiChannel::TRADES, SsiMarket::from(self.symbol.as_str()))).id()
    }

    fn sequence(&self) -> Option<SsiSequence> {
        let day = NaiveDate::parse_from_str(self.trading_date.trim(), "%d/%m/%Y")
            .map(|date| date.num_days_from_ce())
            .unwrap_or_default();
        Some((day, self.total_vol as u64))
    }
}

impl<InstrumentKey> From<(ExchangeId, InstrumentKey, SsiTrade)>
    for MarketIter<InstrumentKey, PublicTrade>
{
    fn from((exchange_id, instrument, trade): (ExchangeId, InstrumentKey, SsiTrade)) -> Self {
        if trade.last_vol <= 0.0 || trade.last_price <= 0.0 {
            return Self(vec![]);
        }

        let Some(side) = trade.aggressor_side() else {
            debug!(
                ?trade,
                "skipping SSI trade without aggressor side (eg/ ATO/ATC auction)"
            );
            return Self(vec![]);
        };

        let time_received = Utc::now();
        Self(vec![Ok(MarketEvent {
            time_exchange: ssi_datetime_utc(&trade.trading_date, &trade.time)
                .unwrap_or(time_received),
            time_received,
            exchange: exchange_id,
            instrument,
            kind: PublicTrade {
                id: format!("{}-{}", trade.trading_date, trade.total_vol as u64),
                price: trade.last_price,
                amount: trade.last_vol,
                side,
            },
        })])
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    const TRADE: &str = r#"{
        "RType":"Trade","TradingDate":"15/11/2026","Time":"09:15:03","Isin":"VN000000VNM1",
        "Symbol":"VNM","Ceiling":69900.0,"Floor":60900.0,"RefPrice":65400.0,"AvgPrice":65512.3,
        "PriorVal":65400.0,"LastPrice":65500.0,"LastVol":200.0,"TotalVal":1310200000.0,"TotalVol":20000.0,
        "MarketId":"HOSE","Exchange":"HOSE","TradingSession":"LO","TradingStatus":"N",
        "Change":100.0,"RatioChange":0.15,"EstMatchedPrice":0.0,"Highest":65600.0,"Lowest":65300.0,
        "Side":"BU"
    }"#;

    #[test]
    fn test_de_ssi_trade() {
        let trade = serde_json::from_str::<SsiTrade>(TRADE).unwrap();
        assert_eq!(
            trade,
            SsiTrade {
                symbol: SmolStr::new("VNM"),
                trading_date: "15/11/2026".to_string(),
                time: "09:15:03".to_string(),
                last_price: 65500.0,
                last_vol: 200.0,
                total_vol: 20000.0,
                side: Some("BU".to_string()),
            }
        );
        assert_eq!(trade.subscription_id(), SubscriptionId::from("X-TRADE|VNM"));
        assert_eq!(trade.aggressor_side(), Some(Side::Buy));
    }

    #[test]
    fn test_de_ssi_trade_string_and_null_numbers() {
        let trade = serde_json::from_str::<SsiTrade>(
            r#"{"Symbol":"hpg","LastPrice":"25300","LastVol":null,"Side":"SD"}"#,
        )
        .unwrap();
        assert_eq!(trade.last_price, 25300.0);
        assert_eq!(trade.last_vol, 0.0);
        assert_eq!(trade.subscription_id(), SubscriptionId::from("X-TRADE|HPG"));
        assert_eq!(trade.aggressor_side(), Some(Side::Sell));
    }

    #[test]
    fn test_ssi_trade_to_market_event() {
        let trade = serde_json::from_str::<SsiTrade>(TRADE).unwrap();
        let events = MarketIter::<u8, PublicTrade>::from((ExchangeId::Ssi, 7, trade)).0;
        assert_eq!(events.len(), 1);

        let event = events.into_iter().next().unwrap().unwrap();
        assert_eq!(event.exchange, ExchangeId::Ssi);
        assert_eq!(event.instrument, 7);
        assert_eq!(
            event.time_exchange,
            NaiveDate::from_ymd_opt(2026, 11, 15)
                .unwrap()
                .and_hms_opt(2, 15, 3)
                .unwrap()
                .and_utc()
        );
        assert_eq!(
            event.kind,
            PublicTrade {
                id: "15/11/2026-20000".to_string(),
                price: 65500.0,
                amount: 200.0,
                side: Side::Buy,
            }
        );
    }

    #[test]
    fn test_ssi_trade_without_side_or_volume_is_skipped() {
        let mut trade = serde_json::from_str::<SsiTrade>(TRADE).unwrap();
        trade.side = Some(String::new());
        assert!(
            MarketIter::<u8, PublicTrade>::from((ExchangeId::Ssi, 0, trade.clone()))
                .0
                .is_empty()
        );

        trade.side = Some("BU".to_string());
        trade.last_vol = 0.0;
        assert!(
            MarketIter::<u8, PublicTrade>::from((ExchangeId::Ssi, 0, trade))
                .0
                .is_empty()
        );
    }
}
