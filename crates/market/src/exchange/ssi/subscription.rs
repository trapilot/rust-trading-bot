use serde::{Deserialize, Serialize};
use serde_json::Value;
use trading_bot_integration::{Validator, error::SocketError};

/// [`Ssi`](super::Ssi) SignalR hub invocation result, received in response to the
/// `SwitchChannels` subscription request.
///
/// ### Raw Payload Examples
/// #### Subscription Success
/// ```json
/// {"I":"0"}
/// {"I":"0","R":true}
/// ```
///
/// #### Subscription Failure
/// ```json
/// {"I":"0","E":"Channel is invalid"}
/// ```
#[derive(Clone, PartialEq, Debug, Deserialize, Serialize)]
pub struct SsiSubResponse {
    #[serde(rename = "I")]
    pub invocation_id: SsiInvocationId,
    #[serde(rename = "R", default, skip_serializing_if = "Option::is_none")]
    pub result: Option<Value>,
    #[serde(rename = "E", default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// SignalR invocation id, sent by servers as either a string or a number.
#[derive(Clone, Eq, PartialEq, Debug, Deserialize, Serialize)]
#[serde(untagged)]
pub enum SsiInvocationId {
    Str(String),
    Num(u64),
}

impl Validator for SsiSubResponse {
    type Error = SocketError;

    fn validate(self) -> Result<Self, SocketError>
    where
        Self: Sized,
    {
        match &self.error {
            None => Ok(self),
            Some(error) => Err(SocketError::Subscribe(format!(
                "received failure subscription response: {error}"
            ))),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_de_ssi_sub_response() {
        struct TestCase {
            input: &'static str,
            expected: Option<SsiSubResponse>,
        }

        let cases = vec![
            TestCase {
                // TC0: success with string id
                input: r#"{"I":"0"}"#,
                expected: Some(SsiSubResponse {
                    invocation_id: SsiInvocationId::Str("0".to_string()),
                    result: None,
                    error: None,
                }),
            },
            TestCase {
                // TC1: success with numeric id and result
                input: r#"{"I":0,"R":true}"#,
                expected: Some(SsiSubResponse {
                    invocation_id: SsiInvocationId::Num(0),
                    result: Some(Value::Bool(true)),
                    error: None,
                }),
            },
            TestCase {
                // TC2: failure
                input: r#"{"I":"0","E":"Channel is invalid"}"#,
                expected: Some(SsiSubResponse {
                    invocation_id: SsiInvocationId::Str("0".to_string()),
                    result: None,
                    error: Some("Channel is invalid".to_string()),
                }),
            },
            TestCase {
                // TC3: keep-alive is not a subscription response
                input: r#"{}"#,
                expected: None,
            },
            TestCase {
                // TC4: broadcast is not a subscription response
                input: r#"{"C":"d-1","M":[{"H":"FcMarketDataV2Hub","M":"Broadcast","A":[]}]}"#,
                expected: None,
            },
        ];

        for (index, test) in cases.into_iter().enumerate() {
            let actual = serde_json::from_str::<SsiSubResponse>(test.input).ok();
            assert_eq!(actual, test.expected, "TC{index} failed");
        }
    }

    #[test]
    fn test_validate_ssi_sub_response() {
        let ok = SsiSubResponse {
            invocation_id: SsiInvocationId::Str("0".to_string()),
            result: None,
            error: None,
        };
        assert!(ok.validate().is_ok());

        let err = SsiSubResponse {
            invocation_id: SsiInvocationId::Str("0".to_string()),
            result: None,
            error: Some("Channel is invalid".to_string()),
        };
        assert!(err.validate().is_err());
    }
}
