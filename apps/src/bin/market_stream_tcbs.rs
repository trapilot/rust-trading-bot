//! Stream live TCBS (Techcombank Securities, Vietnam stock market) trades & top-of-book quotes.
//!
//! Requires a TCBS iFlash Open API token, either directly:
//! ```bash
//! TCBS_TOKEN=... cargo run -p trading-bot-apps --bin market_stream_tcbs
//! ```
//! or as an API key (generated on TCInvest) plus a Smart OTP from the TCInvest app:
//! ```bash
//! TCBS_API_KEY=... TCBS_OTP=123456 cargo run -p trading-bot-apps --bin market_stream_tcbs
//! ```
//! Optional: `TCBS_REST_URL`, `TCBS_STREAM_URL` to override the default servers.

use futures::StreamExt;
use tracing::{info, warn};
use trading_bot_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use trading_bot_market::{
    exchange::tcbs::Tcbs,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::{book::OrderBooksL1, trade::PublicTrades},
};
use trading_bot_runtime::logging::init_logging;

const SYMBOLS: [&str; 4] = ["vnm", "hpg", "fpt", "shb"];

#[tokio::main]
async fn main() {
    init_logging();

    // Each subscribe() batch opens one WebSocket connection
    let trades = Streams::<PublicTrades>::builder()
        .subscribe(SYMBOLS.map(|symbol| {
            (
                Tcbs,
                symbol,
                "vnd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            )
        }))
        .init()
        .await
        .expect("failed to initialise TCBS PublicTrades stream");

    let quotes = Streams::<OrderBooksL1>::builder()
        .subscribe(SYMBOLS.map(|symbol| {
            (
                Tcbs,
                symbol,
                "vnd",
                MarketDataInstrumentKind::Spot,
                OrderBooksL1,
            )
        }))
        .init()
        .await
        .expect("failed to initialise TCBS OrderBooksL1 stream");

    let mut trades = trades
        .select_all()
        .with_error_handler(|error| warn!(?error, "TCBS trade stream error"));
    let mut quotes = quotes
        .select_all()
        .with_error_handler(|error| warn!(?error, "TCBS quote stream error"));

    loop {
        tokio::select! {
            Some(event) = trades.next() => info!(?event, "trade"),
            Some(event) = quotes.next() => info!(?event, "quote"),
            else => break,
        }
    }
}
