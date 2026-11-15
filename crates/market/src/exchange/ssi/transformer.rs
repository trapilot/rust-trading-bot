use super::{Ssi, message::SsiFrame};
use crate::{
    error::DataError,
    event::{MarketEvent, MarketIter},
    exchange::Connector,
    subscription::{Map, SubscriptionKind},
    transformer::ExchangeTransformer,
};
use async_trait::async_trait;
use fnv::FnvHashMap;
use serde::de::DeserializeOwned;
use std::marker::PhantomData;
use tokio::sync::mpsc;
use trading_bot_instrument::exchange::ExchangeId;
use trading_bot_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};

/// An SSI broadcast payload (eg/ an `X-TRADE` or `X-QUOTE` message content).
pub trait SsiPayload {
    /// [`SubscriptionId`] (eg/ "X-TRADE|VNM") used to look up the associated instrument.
    fn subscription_id(&self) -> SubscriptionId;

    /// Optional strictly-increasing per-instrument sequence. When provided, payloads whose
    /// sequence is not greater than the last one seen for the instrument are dropped as
    /// duplicates.
    fn sequence(&self) -> Option<SsiSequence> {
        None
    }
}

/// Per-instrument payload ordering key: `(trading day, intraday sequence)`.
pub type SsiSequence = (i32, u64);

/// [`ExchangeTransformer`] for [`Ssi`] SignalR frames.
///
/// Unlike [`StatelessTransformer`](crate::transformer::stateless::StatelessTransformer), a single
/// SignalR frame can carry payloads for several instruments, so each payload is routed to its
/// instrument individually.
#[derive(Clone, Eq, PartialEq, Debug)]
pub struct SsiTransformer<InstrumentKey, Kind, Payload> {
    instrument_map: Map<InstrumentKey>,
    last_sequence: FnvHashMap<SubscriptionId, SsiSequence>,
    phantom: PhantomData<(Kind, Payload)>,
}

#[async_trait]
impl<InstrumentKey, Kind, Payload> ExchangeTransformer<Ssi, InstrumentKey, Kind>
    for SsiTransformer<InstrumentKey, Kind, Payload>
where
    InstrumentKey: Clone + Send,
    Kind: SubscriptionKind + Send,
    Payload: SsiPayload + DeserializeOwned + Send,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, Payload)>,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        _: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self {
            instrument_map,
            last_sequence: FnvHashMap::default(),
            phantom: PhantomData,
        })
    }
}

impl<InstrumentKey, Kind, Payload> Transformer for SsiTransformer<InstrumentKey, Kind, Payload>
where
    InstrumentKey: Clone,
    Kind: SubscriptionKind,
    Payload: SsiPayload + DeserializeOwned,
    MarketIter<InstrumentKey, Kind::Event>: From<(ExchangeId, InstrumentKey, Payload)>,
{
    type Error = DataError;
    type Input = SsiFrame<Payload>;
    type Output = MarketEvent<InstrumentKey, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let mut output = Vec::with_capacity(input.0.len());

        for payload in input.0 {
            let subscription_id = payload.subscription_id();

            let instrument = match self.instrument_map.find(&subscription_id) {
                Ok(instrument) => instrument.clone(),
                Err(unidentifiable) => {
                    output.push(Err(DataError::from(unidentifiable)));
                    continue;
                }
            };

            if let Some(sequence) = payload.sequence() {
                match self.last_sequence.get_mut(&subscription_id) {
                    Some(last) if sequence <= *last => continue,
                    Some(last) => *last = sequence,
                    None => {
                        self.last_sequence.insert(subscription_id, sequence);
                    }
                }
            }

            output.extend(
                MarketIter::<InstrumentKey, Kind::Event>::from((Ssi::ID, instrument, payload)).0,
            );
        }

        output
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{
        exchange::ssi::trade::SsiTrade,
        subscription::trade::{PublicTrade, PublicTrades},
    };

    fn broadcast(trades: &[(&str, f64, f64)]) -> SsiFrame<SsiTrade> {
        let args = trades
            .iter()
            .map(|(symbol, last_vol, total_vol)| {
                let content = serde_json::json!({
                    "Symbol": symbol, "TradingDate": "15/11/2026", "Time": "09:15:03",
                    "LastPrice": 65500.0, "LastVol": last_vol, "TotalVol": total_vol, "Side": "BU",
                });
                serde_json::Value::String(
                    serde_json::json!({ "DataType": "X-TRADE", "Content": content.to_string() })
                        .to_string(),
                )
            })
            .collect::<Vec<_>>();

        serde_json::from_value(serde_json::json!({
            "C": "d-1",
            "M": [{ "H": "FcMarketDataV2Hub", "M": "Broadcast", "A": args }],
        }))
        .unwrap()
    }

    fn transformer() -> SsiTransformer<u8, PublicTrades, SsiTrade> {
        SsiTransformer {
            instrument_map: Map::from_iter([
                (SubscriptionId::from("X-TRADE|VNM"), 1),
                (SubscriptionId::from("X-TRADE|HPG"), 2),
            ]),
            last_sequence: FnvHashMap::default(),
            phantom: PhantomData,
        }
    }

    fn amounts(output: &[Result<MarketEvent<u8, PublicTrade>, DataError>]) -> Vec<(u8, f64)> {
        output
            .iter()
            .filter_map(|event| event.as_ref().ok())
            .map(|event| (event.instrument, event.kind.amount))
            .collect()
    }

    #[test]
    fn test_ssi_transformer_routes_each_payload_to_its_instrument() {
        let mut transformer = transformer();
        let output =
            transformer.transform(broadcast(&[("VNM", 100.0, 100.0), ("HPG", 50.0, 50.0)]));
        assert_eq!(amounts(&output), vec![(1, 100.0), (2, 50.0)]);
    }

    #[test]
    fn test_ssi_transformer_drops_duplicate_and_stale_trades() {
        let mut transformer = transformer();

        let output = transformer.transform(broadcast(&[
            ("VNM", 100.0, 100.0),
            ("VNM", 100.0, 100.0), // same match re-broadcast (eg/ seller side)
            ("VNM", 30.0, 130.0),
            ("VNM", 10.0, 110.0), // stale
            ("HPG", 50.0, 100.0), // independent sequence per instrument
        ]));
        assert_eq!(amounts(&output), vec![(1, 100.0), (1, 30.0), (2, 50.0)]);
    }

    #[test]
    fn test_ssi_transformer_unidentifiable_symbol() {
        let mut transformer = transformer();
        let output = transformer.transform(broadcast(&[("FPT", 10.0, 10.0)]));
        assert_eq!(output.len(), 1);
        assert!(matches!(
            &output[0],
            Err(DataError::Socket(msg)) if msg.contains("X-TRADE|FPT")
        ));
    }
}
