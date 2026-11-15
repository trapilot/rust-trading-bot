use crate::books::Level;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};
use smol_str::SmolStr;
use std::str::FromStr;
use trading_bot_integration::{
    Validator,
    error::SocketError,
    protocol::{
        StreamParser,
        websocket::{
            WebSocket, WsError, WsMessage, process_close_frame, process_frame, process_ping,
            process_pong,
        },
    },
};

/// Client heartbeat text frame. TCBS closes the connection after 15s without one.
pub const TCBS_HEARTBEAT: &str = "d|p|||";

/// Control code of the authentication response (`d|0|{"success":..}`).
pub const TCBS_CONTROL_AUTH: &str = "0";

/// Control code of the heartbeat timeout config (`d|33|15`).
pub const TCBS_CONTROL_TIMEOUT: &str = "33";

/// Control code of a subscription acknowledgement (`d|34|1`).
pub const TCBS_CONTROL_SUBSCRIBED: &str = "34";

/// Price board channel id of the top 3 bids (`bi`).
pub const TCBS_CHANNEL_BID_INFO: &str = "1";

/// Price board channel id of the top 3 offers (`op`).
pub const TCBS_CHANNEL_OFFER_INFO: &str = "2";

/// Price board channel id of a ticker match, ie/ a trade (`tm`).
pub const TCBS_CHANNEL_TICKER_MATCH: &str = "6";

/// Number of price levels per side published on the price board.
pub const TCBS_BOOK_DEPTH: usize = 3;

/// Build the WebSocket authentication frame: `d|a|||<base64(token)>`.
pub fn auth_frame(token: &str) -> WsMessage {
    use base64::Engine;
    let encoded = base64::engine::general_purpose::STANDARD.encode(token);
    WsMessage::text(format!("d|a|||{encoded}"))
}

/// One decoded [`Tcbs`](super::Tcbs) price board text frame.
///
/// ### Raw Payload Examples
/// See docs: <https://developers.tcbs.com.vn/docs/v1.0.0/stock/ws-price-board/>
/// ```text
/// d|0|{"success":true,"error":null}
/// d|33|15
/// d|34|1
/// d|p|||
/// s|1|{"symbol":"TCB","bidPrice01":"24550","bidPrice02":"24500","bidPrice03":"24450","bidQtty01":"137200","bidQtty02":"354000","bidQtty03":"292900"}
/// s|2|{"symbol":"TCB","offerPrice01":"24600","offerPrice02":"24650","offerPrice03":"24700","offerQtty01":"294000","offerQtty02":"163400","offerQtty03":"743600"}
/// s|6|{"symbol":"TCB","matchPrice":"24550","matchQtty":"5000","change":200.0,"changePercent":0.82,"totalVolume":"9745500","totalValue":"240774655000"}
/// ```
#[derive(Clone, PartialEq, Debug)]
pub enum TcbsFrame {
    /// `d|<code>|<payload>` control frame (auth response, timeout config, ack, heartbeat).
    Control(TcbsControl),
    /// `s|<channel>|<json>` data frame.
    Data(TcbsData),
}

/// `d|<code>|<payload>` control frame.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct TcbsControl {
    pub code: SmolStr,
    pub payload: String,
}

/// `s|<channel>|<json>` data frame, decoded for the channels this integration uses.
#[derive(Clone, PartialEq, Debug)]
pub enum TcbsData {
    /// Channel 1: top 3 bids. Replaces the whole bid side.
    BidInfo(TcbsBookSide),
    /// Channel 2: top 3 offers. Replaces the whole ask side.
    OfferInfo(TcbsBookSide),
    /// Channel 6: a matched trade.
    TickerMatch(TcbsTickerMatch),
    /// Any other channel (eg/ foreign room, base price), not decoded.
    Other { channel: SmolStr },
}

impl TcbsData {
    /// Symbol this data frame refers to, if decoded.
    pub fn symbol(&self) -> Option<&str> {
        match self {
            TcbsData::BidInfo(side) | TcbsData::OfferInfo(side) => Some(&side.symbol),
            TcbsData::TickerMatch(trade) => Some(&trade.symbol),
            TcbsData::Other { .. } => None,
        }
    }
}

/// One side of the [`Tcbs`](super::Tcbs) top-3 order book for a symbol. Empty levels (price or
/// quantity of 0, or non-numeric prices such as "ATO"/"ATC") are dropped.
#[derive(Clone, PartialEq, Debug, Serialize)]
pub struct TcbsBookSide {
    pub symbol: SmolStr,
    pub levels: Vec<Level>,
}

impl TcbsBookSide {
    fn parse(fields: &Map<String, Value>, price: &str, quantity: &str) -> Result<Self, String> {
        let symbol = symbol(fields)?;
        let levels = (1..=TCBS_BOOK_DEPTH)
            .filter_map(|depth| {
                let price = value_to_decimal(fields.get(&format!("{price}{depth:02}"))?)?;
                let amount = value_to_decimal(fields.get(&format!("{quantity}{depth:02}"))?)?;
                (price > Decimal::ZERO && amount > Decimal::ZERO).then(|| Level::new(price, amount))
            })
            .collect();

        Ok(Self { symbol, levels })
    }
}

/// [`Tcbs`](super::Tcbs) ticker match (trade). Prices & quantities arrive as strings.
///
/// TCBS does not publish a trade id, timestamp or aggressor side for matches.
#[derive(Clone, PartialEq, Debug, Serialize)]
pub struct TcbsTickerMatch {
    pub symbol: SmolStr,
    pub price: f64,
    pub quantity: f64,
    /// Cumulative matched volume for the session, if provided.
    pub total_volume: Option<u64>,
}

impl TcbsTickerMatch {
    fn parse(fields: &Map<String, Value>) -> Result<Self, String> {
        Ok(Self {
            symbol: symbol(fields)?,
            price: value_to_f64(fields.get("matchPrice")).unwrap_or_default(),
            quantity: value_to_f64(fields.get("matchQtty")).unwrap_or_default(),
            total_volume: value_to_f64(fields.get("totalVolume"))
                .filter(|volume| *volume >= 0.0)
                .map(|volume| volume as u64),
        })
    }
}

impl FromStr for TcbsFrame {
    type Err = String;

    fn from_str(frame: &str) -> Result<Self, Self::Err> {
        let (kind, rest) = frame
            .split_once('|')
            .ok_or_else(|| format!("missing frame type in: {frame}"))?;

        match kind {
            "d" => {
                let (code, payload) = rest.split_once('|').unwrap_or((rest, ""));
                Ok(TcbsFrame::Control(TcbsControl {
                    code: SmolStr::new(code),
                    payload: payload.to_string(),
                }))
            }
            "s" => {
                let (channel, payload) = rest
                    .split_once('|')
                    .ok_or_else(|| format!("missing data payload in: {frame}"))?;

                let data = match channel {
                    TCBS_CHANNEL_BID_INFO => TcbsData::BidInfo(TcbsBookSide::parse(
                        &json(payload)?,
                        "bidPrice",
                        "bidQtty",
                    )?),
                    TCBS_CHANNEL_OFFER_INFO => TcbsData::OfferInfo(TcbsBookSide::parse(
                        &json(payload)?,
                        "offerPrice",
                        "offerQtty",
                    )?),
                    TCBS_CHANNEL_TICKER_MATCH => {
                        TcbsData::TickerMatch(TcbsTickerMatch::parse(&json(payload)?)?)
                    }
                    other => TcbsData::Other {
                        channel: SmolStr::new(other),
                    },
                };
                Ok(TcbsFrame::Data(data))
            }
            other => Err(format!("unknown frame type '{other}' in: {frame}")),
        }
    }
}

fn json(payload: &str) -> Result<Map<String, Value>, String> {
    serde_json::from_str(payload).map_err(|error| format!("invalid JSON payload: {error}"))
}

fn symbol(fields: &Map<String, Value>) -> Result<SmolStr, String> {
    fields
        .get("symbol")
        .and_then(Value::as_str)
        .map(SmolStr::new)
        .ok_or_else(|| "missing field `symbol`".to_string())
}

/// Interpret a TCBS numeric field (a JSON number or a numeric string) as exact [`Decimal`].
pub fn value_to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        Value::Number(number) => {
            let raw = number.to_string();
            Decimal::from_str(&raw)
                .or_else(|_| Decimal::from_scientific(&raw))
                .ok()
        }
        Value::String(raw) => Decimal::from_str(raw.trim()).ok(),
        _ => None,
    }
}

fn value_to_f64(value: Option<&Value>) -> Option<f64> {
    match value? {
        Value::Number(number) => number.as_f64(),
        Value::String(raw) => raw.trim().parse().ok(),
        _ => None,
    }
}

/// [`StreamParser`] for the [`Tcbs`](super::Tcbs) pipe-delimited text protocol.
#[derive(Copy, Clone, Eq, PartialEq, Ord, PartialOrd, Hash, Debug, Deserialize, Serialize)]
pub struct TcbsParser;

impl StreamParser<TcbsFrame> for TcbsParser {
    type Stream = WebSocket;
    type Message = WsMessage;
    type Error = WsError;

    fn parse(input: Result<Self::Message, Self::Error>) -> Option<Result<TcbsFrame, SocketError>> {
        match input {
            Ok(WsMessage::Text(text)) => Some(parse_text(text.as_str())),
            Ok(WsMessage::Binary(_)) => None,
            Ok(WsMessage::Ping(ping)) => process_ping(ping),
            Ok(WsMessage::Pong(pong)) => process_pong(pong),
            Ok(WsMessage::Close(close_frame)) => process_close_frame(close_frame),
            Ok(WsMessage::Frame(frame)) => process_frame(frame),
            Err(error) => Some(Err(SocketError::WebSocket(Box::new(error)))),
        }
    }
}

/// Parse one text frame, mapping failures to [`SocketError::Exchange`] with the raw payload.
pub fn parse_text(text: &str) -> Result<TcbsFrame, SocketError> {
    TcbsFrame::from_str(text).map_err(|error| {
        SocketError::Exchange(format!("failed to parse TCBS frame ({error}): {text}"))
    })
}

/// [`Tcbs`](super::Tcbs) authentication response, the payload of `d|0|{...}`.
///
/// ```json
/// {"success":true,"error":null}
/// {"success":false,"error":{"code":"211110","message":"Invalid JWT"}}
/// ```
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct TcbsAuthResponse {
    pub success: bool,
    #[serde(default)]
    pub error: Option<TcbsError>,
}

/// [`Tcbs`](super::Tcbs) error payload.
#[derive(Clone, PartialEq, Eq, Debug, Deserialize, Serialize)]
pub struct TcbsError {
    #[serde(default)]
    pub code: Option<String>,
    #[serde(default)]
    pub message: Option<String>,
}

impl Validator for TcbsAuthResponse {
    type Error = SocketError;

    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        if self.success {
            Ok(self)
        } else {
            let (code, message) = self
                .error
                .map(|error| (error.code, error.message))
                .unwrap_or_default();
            Err(SocketError::Subscribe(format!(
                "TCBS authentication failed: code={} message={}",
                code.unwrap_or_default(),
                message.unwrap_or_default()
            )))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;

    #[test]
    fn test_auth_frame() {
        // Dummy value: never put real or docs-sample tokens in source (secret scanners flag them)
        let token = "FAKE-TCBS-TEST-TOKEN";
        assert_eq!(
            auth_frame(token).to_text().unwrap(),
            "d|a|||RkFLRS1UQ0JTLVRFU1QtVE9LRU4="
        );
    }

    #[test]
    fn test_parse_control_frames() {
        let control = |code: &str, payload: &str| {
            TcbsFrame::Control(TcbsControl {
                code: SmolStr::new(code),
                payload: payload.to_string(),
            })
        };

        assert_eq!(
            "d|0|{\"success\":true,\"error\":null}".parse::<TcbsFrame>(),
            Ok(control("0", "{\"success\":true,\"error\":null}"))
        );
        assert_eq!("d|33|15".parse::<TcbsFrame>(), Ok(control("33", "15")));
        assert_eq!("d|34|1".parse::<TcbsFrame>(), Ok(control("34", "1")));
        assert_eq!("d|p|||".parse::<TcbsFrame>(), Ok(control("p", "||")));
        assert!("garbage".parse::<TcbsFrame>().is_err());
        assert!("x|1|{}".parse::<TcbsFrame>().is_err());
    }

    #[test]
    fn test_parse_bid_and_offer_frames() {
        let bid = r#"s|1|{"symbol":"TCB","bidPrice01":"24550","bidPrice02":"24500","bidPrice03":"0","bidQtty01":"137200","bidQtty02":"354000","bidQtty03":"0"}"#;
        assert_eq!(
            bid.parse::<TcbsFrame>(),
            Ok(TcbsFrame::Data(TcbsData::BidInfo(TcbsBookSide {
                symbol: SmolStr::new("TCB"),
                levels: vec![
                    Level::new(dec!(24550), dec!(137200)),
                    Level::new(dec!(24500), dec!(354000)),
                ],
            })))
        );

        let offer = r#"s|2|{"symbol":"TCB","offerPrice01":"ATO","offerPrice02":24650,"offerPrice03":"24700","offerQtty01":"294000","offerQtty02":163400,"offerQtty03":"743600"}"#;
        assert_eq!(
            offer.parse::<TcbsFrame>(),
            Ok(TcbsFrame::Data(TcbsData::OfferInfo(TcbsBookSide {
                symbol: SmolStr::new("TCB"),
                levels: vec![
                    Level::new(dec!(24650), dec!(163400)),
                    Level::new(dec!(24700), dec!(743600)),
                ],
            })))
        );
    }

    #[test]
    fn test_parse_ticker_match_frame() {
        let frame = r#"s|6|{"symbol":"TCB","matchPrice":"24550","matchQtty":"5000","change":200.0,"changePercent":0.8213552361396304,"totalVolume":"9745500","totalValue":"240774655000"}"#;
        assert_eq!(
            frame.parse::<TcbsFrame>(),
            Ok(TcbsFrame::Data(TcbsData::TickerMatch(TcbsTickerMatch {
                symbol: SmolStr::new("TCB"),
                price: 24550.0,
                quantity: 5000.0,
                total_volume: Some(9745500),
            })))
        );
    }

    #[test]
    fn test_parse_other_and_invalid_data_frames() {
        assert_eq!(
            r#"s|4|{"symbol":"VPD","ceilPrice":25000}"#.parse::<TcbsFrame>(),
            Ok(TcbsFrame::Data(TcbsData::Other {
                channel: SmolStr::new("4")
            }))
        );
        assert!(r#"s|6|not json"#.parse::<TcbsFrame>().is_err());
        assert!(r#"s|1|{"bidPrice01":"1"}"#.parse::<TcbsFrame>().is_err());
        assert!("s|6".parse::<TcbsFrame>().is_err());
    }

    #[test]
    fn test_parser_skips_non_text() {
        assert!(
            <TcbsParser as StreamParser<TcbsFrame>>::parse(Ok(WsMessage::binary(vec![1u8])))
                .is_none()
        );
        assert!(matches!(
            <TcbsParser as StreamParser<TcbsFrame>>::parse(Ok(WsMessage::text("d|34|1"))),
            Some(Ok(TcbsFrame::Control(_)))
        ));
        assert!(matches!(
            <TcbsParser as StreamParser<TcbsFrame>>::parse(Ok(WsMessage::text("???"))),
            Some(Err(SocketError::Exchange(_)))
        ));
    }

    #[test]
    fn test_validate_auth_response() {
        let ok: TcbsAuthResponse =
            serde_json::from_str(r#"{"success":true,"error":null}"#).unwrap();
        assert!(ok.validate().is_ok());

        let err: TcbsAuthResponse = serde_json::from_str(
            r#"{"success":false,"error":{"code":"211110","message":"Invalid JWT"}}"#,
        )
        .unwrap();
        assert!(matches!(
            err.validate(),
            Err(SocketError::Subscribe(msg)) if msg.contains("211110") && msg.contains("Invalid JWT")
        ));
    }
}
