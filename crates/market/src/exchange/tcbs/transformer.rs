//! Stateful [`ExchangeTransformer`] for [`Tcbs`].
//!
//! The TCBS price board pushes bids (`s|1`), offers (`s|2`) and matches (`s|6`) as separate
//! frames, so per-instrument state is kept to:
//! - Merge the two book sides into [`OrderBookL1`] / [`OrderBookEvent::Snapshot`] events.
//! - Infer the aggressor [`Side`] of trades, which TCBS does not publish: the quote rule is used
//!   against the last known book (price at/above the best ask -> buy, at/below the best bid ->
//!   sell), falling back to the tick rule (up-tick -> buy, down-tick -> sell, unchanged -> the
//!   previous side). Treat it as an estimate.
//!
//! TCBS frames carry no timestamp either, so `time_exchange` is the time the frame was received.

use super::{
    Tcbs,
    channel::TcbsChannel,
    market::TcbsMarket,
    message::{TcbsData, TcbsFrame, TcbsTickerMatch},
};
use crate::{
    Identifier,
    books::{Level, OrderBook},
    error::DataError,
    event::MarketEvent,
    exchange::{Connector, ExchangeSub},
    subscription::{
        Map, SubscriptionKind,
        book::{OrderBookEvent, OrderBookL1, OrderBooksL1, OrderBooksL2},
        trade::{PublicTrade, PublicTrades},
    },
    transformer::ExchangeTransformer,
};
use async_trait::async_trait;
use chrono::{DateTime, Utc};
use fnv::FnvHashMap;
use rust_decimal::{Decimal, prelude::FromPrimitive};
use std::marker::PhantomData;
use tokio::sync::mpsc;
use trading_bot_instrument::Side;
use trading_bot_integration::{
    Transformer, protocol::websocket::WsMessage, subscription::SubscriptionId,
};

/// Latest known market state for one TCBS instrument.
#[derive(Clone, PartialEq, Debug, Default)]
pub struct TcbsInstrumentState {
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub last_trade_price: Option<f64>,
    pub last_trade_side: Option<Side>,
    pub last_total_volume: Option<u64>,
    pub book_sequence: u64,
}

impl TcbsInstrumentState {
    pub fn best_bid(&self) -> Option<Level> {
        self.bids.iter().max_by_key(|level| level.price).copied()
    }

    pub fn best_ask(&self) -> Option<Level> {
        self.asks.iter().min_by_key(|level| level.price).copied()
    }

    fn apply_book(&mut self, data: &TcbsData) -> bool {
        match data {
            TcbsData::BidInfo(side) => self.bids = side.levels.clone(),
            TcbsData::OfferInfo(side) => self.asks = side.levels.clone(),
            _ => return false,
        }
        self.book_sequence += 1;
        true
    }

    /// Infer the aggressor side of `trade` (quote rule, then tick rule).
    pub fn infer_side(&self, trade: &TcbsTickerMatch) -> Side {
        let price = Decimal::from_f64(trade.price);

        let by_quote = match (price, self.best_bid(), self.best_ask()) {
            (Some(price), _, Some(ask)) if price >= ask.price => Some(Side::Buy),
            (Some(price), Some(bid), _) if price <= bid.price => Some(Side::Sell),
            _ => None,
        };

        let by_tick = match self.last_trade_price {
            Some(last) if trade.price > last => Some(Side::Buy),
            Some(last) if trade.price < last => Some(Side::Sell),
            _ => None,
        };

        by_quote
            .or(by_tick)
            .or(self.last_trade_side)
            .unwrap_or(Side::Buy)
    }
}

/// How a [`SubscriptionKind`] turns TCBS price board data into its normalised event.
pub trait TcbsSubscriptionKind: SubscriptionKind {
    /// Price board channels subscribed for this kind.
    const CHANNEL: TcbsChannel;

    /// Update `state` with `data` and return the event to emit, if any.
    fn event(state: &mut TcbsInstrumentState, data: &TcbsData) -> Option<Self::Event>;
}

impl TcbsSubscriptionKind for PublicTrades {
    const CHANNEL: TcbsChannel = TcbsChannel::TRADES;

    fn event(state: &mut TcbsInstrumentState, data: &TcbsData) -> Option<PublicTrade> {
        let TcbsData::TickerMatch(trade) = data else {
            state.apply_book(data);
            return None;
        };

        if trade.price <= 0.0 || trade.quantity <= 0.0 {
            return None;
        }

        // Re-broadcast of the same match: cumulative volume did not change
        if trade.total_volume.is_some() && trade.total_volume == state.last_total_volume {
            return None;
        }

        let side = state.infer_side(trade);
        state.last_trade_price = Some(trade.price);
        state.last_trade_side = Some(side);
        state.last_total_volume = trade.total_volume;

        Some(PublicTrade {
            id: trade
                .total_volume
                .map(|volume| volume.to_string())
                .unwrap_or_default(),
            price: trade.price,
            amount: trade.quantity,
            side,
        })
    }
}

impl TcbsSubscriptionKind for OrderBooksL1 {
    const CHANNEL: TcbsChannel = TcbsChannel::QUOTES;

    fn event(state: &mut TcbsInstrumentState, data: &TcbsData) -> Option<OrderBookL1> {
        state.apply_book(data).then(|| OrderBookL1 {
            last_update_time: Utc::now(),
            best_bid: state.best_bid(),
            best_ask: state.best_ask(),
        })
    }
}

impl TcbsSubscriptionKind for OrderBooksL2 {
    const CHANNEL: TcbsChannel = TcbsChannel::QUOTES;

    fn event(state: &mut TcbsInstrumentState, data: &TcbsData) -> Option<OrderBookEvent> {
        state.apply_book(data).then(|| {
            OrderBookEvent::Snapshot(OrderBook::new(
                state.book_sequence,
                Some(Utc::now()),
                state.bids.clone(),
                state.asks.clone(),
            ))
        })
    }
}

/// [`ExchangeTransformer`] for [`Tcbs`], see the [module docs](self).
#[derive(Clone, PartialEq, Debug)]
pub struct TcbsTransformer<InstrumentKey, Kind> {
    instrument_map: Map<InstrumentKey>,
    states: FnvHashMap<SubscriptionId, TcbsInstrumentState>,
    phantom: PhantomData<Kind>,
}

impl<InstrumentKey, Kind> TcbsTransformer<InstrumentKey, Kind> {
    pub fn new(instrument_map: Map<InstrumentKey>) -> Self {
        Self {
            instrument_map,
            states: FnvHashMap::default(),
            phantom: PhantomData,
        }
    }
}

#[async_trait]
impl<InstrumentKey, Kind> ExchangeTransformer<Tcbs, InstrumentKey, Kind>
    for TcbsTransformer<InstrumentKey, Kind>
where
    InstrumentKey: Clone + Send,
    Kind: TcbsSubscriptionKind + Send,
{
    async fn init(
        instrument_map: Map<InstrumentKey>,
        _: &[MarketEvent<InstrumentKey, Kind::Event>],
        _: mpsc::UnboundedSender<WsMessage>,
    ) -> Result<Self, DataError> {
        Ok(Self::new(instrument_map))
    }
}

impl<InstrumentKey, Kind> Transformer for TcbsTransformer<InstrumentKey, Kind>
where
    InstrumentKey: Clone,
    Kind: TcbsSubscriptionKind,
{
    type Error = DataError;
    type Input = TcbsFrame;
    type Output = MarketEvent<InstrumentKey, Kind::Event>;
    type OutputIter = Vec<Result<Self::Output, Self::Error>>;

    fn transform(&mut self, input: Self::Input) -> Self::OutputIter {
        let TcbsFrame::Data(data) = input else {
            // Control frames (heartbeat echo, timeout config, acks)
            return vec![];
        };
        let Some(symbol) = data.symbol() else {
            return vec![];
        };

        let subscription_id = ExchangeSub::from((Kind::CHANNEL, TcbsMarket::from(symbol))).id();
        let instrument = match self.instrument_map.find(&subscription_id) {
            Ok(instrument) => instrument.clone(),
            Err(unidentifiable) => return vec![Err(DataError::from(unidentifiable))],
        };

        let state = self.states.entry(subscription_id).or_default();
        let Some(kind) = Kind::event(state, &data) else {
            return vec![];
        };

        let time_received: DateTime<Utc> = Utc::now();
        vec![Ok(MarketEvent {
            time_exchange: time_received,
            time_received,
            exchange: Tcbs::ID,
            instrument,
            kind,
        })]
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use rust_decimal_macros::dec;
    use trading_bot_instrument::exchange::ExchangeId;

    fn frame(raw: &str) -> TcbsFrame {
        raw.parse().unwrap()
    }

    fn bid(price: &str) -> TcbsFrame {
        frame(&format!(
            r#"s|1|{{"symbol":"TCB","bidPrice01":"{price}","bidPrice02":"24500","bidPrice03":"24450","bidQtty01":"100","bidQtty02":"200","bidQtty03":"300"}}"#
        ))
    }

    fn offer(price: &str) -> TcbsFrame {
        frame(&format!(
            r#"s|2|{{"symbol":"TCB","offerPrice01":"{price}","offerPrice02":"24700","offerPrice03":"24750","offerQtty01":"400","offerQtty02":"500","offerQtty03":"600"}}"#
        ))
    }

    fn trade(price: &str, total: u64) -> TcbsFrame {
        frame(&format!(
            r#"s|6|{{"symbol":"TCB","matchPrice":"{price}","matchQtty":"10","change":0.0,"totalVolume":"{total}"}}"#
        ))
    }

    fn transformer<Kind>(channel: TcbsChannel) -> TcbsTransformer<u8, Kind> {
        TcbsTransformer::new(Map::from_iter([(
            ExchangeSub::from((channel, TcbsMarket::from("TCB"))).id(),
            1,
        )]))
    }

    fn events<Kind: TcbsSubscriptionKind>(
        transformer: &mut TcbsTransformer<u8, Kind>,
        frames: Vec<TcbsFrame>,
    ) -> Vec<Kind::Event> {
        frames
            .into_iter()
            .flat_map(|frame| transformer.transform(frame))
            .map(|event| {
                let event = event.unwrap();
                assert_eq!(event.exchange, ExchangeId::Tcbs);
                assert_eq!(event.instrument, 1);
                event.kind
            })
            .collect()
    }

    #[test]
    fn test_tcbs_l1_merges_bid_and_offer_frames() {
        let mut transformer = transformer::<OrderBooksL1>(TcbsChannel::QUOTES);
        let books = events(
            &mut transformer,
            vec![
                frame("d|34|1"),
                bid("24550"),
                offer("24600"),
                trade("24600", 10),
            ],
        );

        assert_eq!(books.len(), 2);
        assert_eq!(books[0].best_bid, Some(Level::new(dec!(24550), dec!(100))));
        assert_eq!(books[0].best_ask, None);
        assert_eq!(books[1].best_bid, Some(Level::new(dec!(24550), dec!(100))));
        assert_eq!(books[1].best_ask, Some(Level::new(dec!(24600), dec!(400))));
    }

    #[test]
    fn test_tcbs_l2_emits_snapshots_with_sequence() {
        let mut transformer = transformer::<OrderBooksL2>(TcbsChannel::QUOTES);
        let books = events(&mut transformer, vec![bid("24550"), offer("24600")]);

        let OrderBookEvent::Snapshot(book) = &books[1] else {
            panic!("expected snapshot");
        };
        assert_eq!(book.sequence(), 2);
        assert_eq!(book.bids().levels().len(), 3);
        assert_eq!(book.asks().levels().len(), 3);
        assert_eq!(book.bids().levels()[0], Level::new(dec!(24550), dec!(100)));
        assert_eq!(book.asks().levels()[0], Level::new(dec!(24600), dec!(400)));
    }

    #[test]
    fn test_tcbs_trades_infer_side_and_dedupe() {
        let mut transformer = transformer::<PublicTrades>(TcbsChannel::TRADES);
        let trades = events(
            &mut transformer,
            vec![
                bid("24550"),
                offer("24600"),
                trade("24600", 10), // at best ask -> Buy
                trade("24600", 10), // duplicate cumulative volume -> skipped
                trade("24550", 20), // at best bid -> Sell
                offer("24650"),     // spread widens: 24550 / 24650
                trade("24600", 30), // inside spread, up-tick -> Buy
                trade("24600", 40), // inside spread, unchanged -> previous side (Buy)
                trade("24580", 50), // inside spread, down-tick -> Sell
            ],
        );

        assert_eq!(
            trades
                .iter()
                .map(|trade| (trade.id.as_str(), trade.price, trade.side))
                .collect::<Vec<_>>(),
            vec![
                ("10", 24600.0, Side::Buy),
                ("20", 24550.0, Side::Sell),
                ("30", 24600.0, Side::Buy),
                ("40", 24600.0, Side::Buy),
                ("50", 24580.0, Side::Sell),
            ]
        );
        assert!(trades.iter().all(|trade| trade.amount == 10.0));
    }

    #[test]
    fn test_tcbs_unknown_symbol_is_unidentifiable() {
        let mut transformer = transformer::<PublicTrades>(TcbsChannel::TRADES);
        let output = transformer.transform(frame(
            r#"s|6|{"symbol":"FPT","matchPrice":"1","matchQtty":"1"}"#,
        ));
        assert!(matches!(
            output.as_slice(),
            [Err(DataError::Socket(msg))] if msg.contains("FPT")
        ));
    }

    #[test]
    fn test_tcbs_ignores_control_and_other_channels() {
        let mut transformer = transformer::<PublicTrades>(TcbsChannel::TRADES);
        assert!(transformer.transform(frame("d|p|||")).is_empty());
        assert!(
            transformer
                .transform(frame(r#"s|4|{"symbol":"TCB","ceilPrice":1}"#))
                .is_empty()
        );
    }
}
