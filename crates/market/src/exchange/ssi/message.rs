use chrono::{DateTime, FixedOffset, NaiveDate, NaiveTime, Utc};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, de::DeserializeOwned};
use serde_json::Value;
use std::str::FromStr;
use tracing::warn;

/// SignalR hub method SSI uses to push market data.
pub const SSI_SIGNALR_BROADCAST: &str = "Broadcast";

/// SignalR hub method SSI uses to push errors (eg/ invalid channel).
pub const SSI_SIGNALR_ERROR: &str = "Error";

/// Classic ASP.NET SignalR (protocol 1.5) persistent-connection frame.
///
/// ### Raw Payload Examples
/// #### Init / KeepAlive
/// ```json
/// {"C":"d-5A9B1D2C-B,0|C,0|D,1","S":1,"M":[]}
/// {}
/// ```
///
/// #### Hub Broadcast
/// ```json
/// {
///   "C":"d-5A9B1D2C-B,1|C,0|D,1",
///   "M":[{"H":"FcMarketDataV2Hub","M":"Broadcast","A":["{\"DataType\":\"X-TRADE\",\"Content\":\"{...}\"}"]}]
/// }
/// ```
#[derive(Clone, PartialEq, Debug, Default, Deserialize, Serialize)]
pub struct SignalRFrame {
    #[serde(rename = "M", default)]
    pub messages: Vec<SignalRHubMessage>,
}

/// One hub method invocation pushed from the server inside a [`SignalRFrame`].
#[derive(Clone, PartialEq, Debug, Default, Deserialize, Serialize)]
pub struct SignalRHubMessage {
    #[serde(rename = "H", default)]
    pub hub: String,
    #[serde(rename = "M", default)]
    pub method: String,
    #[serde(rename = "A", default)]
    pub args: Vec<Value>,
}

/// SSI broadcast envelope carried in [`SignalRHubMessage::args`].
///
/// Both the envelope itself and its `Content` are usually JSON-encoded strings, but plain JSON
/// objects are accepted too.
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct SsiEnvelope {
    #[serde(rename = "DataType", default)]
    pub data_type: Option<String>,
    #[serde(rename = "Content")]
    pub content: Value,
}

/// Every `T` payload decoded from the SSI `Broadcast` invocations in one [`SignalRFrame`].
///
/// Keep-alive, init and other non-broadcast frames decode to an empty [`SsiFrame`]. Payloads that
/// fail to decode as `T` are logged and skipped, so one malformed message does not drop the frame.
#[derive(Clone, PartialEq, Debug)]
pub struct SsiFrame<T>(pub Vec<T>);

impl<'de, T> Deserialize<'de> for SsiFrame<T>
where
    T: DeserializeOwned,
{
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let frame = SignalRFrame::deserialize(deserializer)?;

        let payloads = frame
            .messages
            .into_iter()
            .filter_map(|message| {
                if message.method.eq_ignore_ascii_case(SSI_SIGNALR_BROADCAST) {
                    Some(message.args)
                } else {
                    if message.method.eq_ignore_ascii_case(SSI_SIGNALR_ERROR) {
                        warn!(args = ?message.args, "SSI SignalR hub pushed an error");
                    }
                    None
                }
            })
            .flatten()
            .filter_map(|arg| {
                decode_broadcast::<T>(arg)
                    .inspect_err(|error| warn!(%error, "failed to decode SSI broadcast payload"))
                    .ok()
            })
            .collect();

        Ok(Self(payloads))
    }
}

/// Decode one `Broadcast` argument (an [`SsiEnvelope`]) into its `Content` payload `T`.
pub fn decode_broadcast<T>(arg: Value) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    let envelope: SsiEnvelope = from_str_or_value(arg)?;
    from_str_or_value(envelope.content)
}

fn from_str_or_value<T>(value: Value) -> Result<T, serde_json::Error>
where
    T: DeserializeOwned,
{
    match value {
        Value::String(raw) => serde_json::from_str(&raw),
        other => serde_json::from_value(other),
    }
}

/// Vietnam (ICT, UTC+7) offset that SSI `TradingDate` & `Time` fields are expressed in.
pub const SSI_UTC_OFFSET_SECONDS: i32 = 7 * 60 * 60;

/// Combine SSI `TradingDate` ("dd/mm/yyyy") & `Time` ("HH:MM:SS", Vietnam local time) into a UTC
/// timestamp.
pub fn ssi_datetime_utc(trading_date: &str, time: &str) -> Option<DateTime<Utc>> {
    let date = NaiveDate::parse_from_str(trading_date.trim(), "%d/%m/%Y").ok()?;
    let time = NaiveTime::parse_from_str(time.trim(), "%H:%M:%S").ok()?;
    let offset = FixedOffset::east_opt(SSI_UTC_OFFSET_SECONDS)?;
    date.and_time(time)
        .and_local_timezone(offset)
        .single()
        .map(|datetime| datetime.with_timezone(&Utc))
}

/// Interpret an SSI numeric field (a JSON number, a numeric string, or null) as [`Decimal`].
pub fn value_to_decimal(value: &Value) -> Option<Decimal> {
    match value {
        // Parse the textual representation so the value is exact (no f64 rounding)
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

/// Deserialize an SSI numeric field that may be a number, numeric string or null as `f64`
/// (null and non-numeric strings become 0.0).
pub fn de_flexible_f64<'de, D>(deserializer: D) -> Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = Value::deserialize(deserializer)?;
    Ok(match &value {
        Value::Number(number) => number.as_f64().unwrap_or_default(),
        Value::String(raw) => raw.trim().parse().unwrap_or_default(),
        _ => 0.0,
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Debug, PartialEq, Deserialize)]
    struct Payload {
        #[serde(rename = "Symbol")]
        symbol: String,
    }

    #[test]
    fn test_de_ssi_frame() {
        struct TestCase {
            input: &'static str,
            expected: Vec<Payload>,
        }

        let cases = vec![
            TestCase {
                // TC0: keep-alive
                input: r#"{}"#,
                expected: vec![],
            },
            TestCase {
                // TC1: init frame
                input: r#"{"C":"d-1,0|B,0","S":1,"M":[]}"#,
                expected: vec![],
            },
            TestCase {
                // TC2: invocation result is not a broadcast
                input: r#"{"I":"0"}"#,
                expected: vec![],
            },
            TestCase {
                // TC3: broadcast with string-encoded envelope & content
                input: r#"{"C":"d-1","M":[{"H":"FcMarketDataV2Hub","M":"Broadcast","A":["{\"DataType\":\"X-TRADE\",\"Content\":\"{\\\"Symbol\\\":\\\"VNM\\\"}\"}"]}]}"#,
                expected: vec![Payload {
                    symbol: "VNM".to_string(),
                }],
            },
            TestCase {
                // TC4: broadcast with object envelope, multiple messages, one malformed
                input: r#"{"M":[
                    {"H":"FcMarketDataV2Hub","M":"Broadcast","A":[{"DataType":"X-TRADE","Content":{"Symbol":"HPG"}}]},
                    {"H":"FcMarketDataV2Hub","M":"Broadcast","A":[{"DataType":"X-TRADE","Content":"not json"}]},
                    {"H":"FcMarketDataV2Hub","M":"Error","A":["bad channel"]},
                    {"H":"FcMarketDataV2Hub","M":"broadcast","A":["{\"Content\":\"{\\\"Symbol\\\":\\\"FPT\\\"}\"}"]}
                ]}"#,
                expected: vec![
                    Payload {
                        symbol: "HPG".to_string(),
                    },
                    Payload {
                        symbol: "FPT".to_string(),
                    },
                ],
            },
        ];

        for (index, test) in cases.into_iter().enumerate() {
            let actual = serde_json::from_str::<SsiFrame<Payload>>(test.input).unwrap();
            assert_eq!(actual.0, test.expected, "TC{index} failed");
        }
    }

    #[test]
    fn test_ssi_datetime_utc() {
        // 09:15:03 in Vietnam (UTC+7) is 02:15:03 UTC
        assert_eq!(
            ssi_datetime_utc("15/11/2026", "09:15:03"),
            Some(
                NaiveDate::from_ymd_opt(2026, 11, 15)
                    .unwrap()
                    .and_hms_opt(2, 15, 3)
                    .unwrap()
                    .and_utc()
            )
        );
        // Crossing midnight backwards
        assert_eq!(
            ssi_datetime_utc("01/01/2026", "06:59:59"),
            Some(
                NaiveDate::from_ymd_opt(2025, 12, 31)
                    .unwrap()
                    .and_hms_opt(23, 59, 59)
                    .unwrap()
                    .and_utc()
            )
        );
        assert_eq!(ssi_datetime_utc("2026-11-15", "09:15:03"), None);
        assert_eq!(ssi_datetime_utc("15/11/2026", ""), None);
    }

    #[test]
    fn test_value_to_decimal() {
        use rust_decimal_macros::dec;
        assert_eq!(
            value_to_decimal(&serde_json::json!(25300)),
            Some(dec!(25300))
        );
        assert_eq!(
            value_to_decimal(&serde_json::json!(25.35)),
            Some(dec!(25.35))
        );
        assert_eq!(
            value_to_decimal(&serde_json::json!("1200")),
            Some(dec!(1200))
        );
        assert_eq!(value_to_decimal(&serde_json::json!("ATO")), None);
        assert_eq!(value_to_decimal(&Value::Null), None);
    }
}
