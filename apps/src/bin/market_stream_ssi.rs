//! Stream live SSI (Vietnam stock market) trades & top-of-book quotes.
//!
//! Requires SSI FastConnect Data credentials:
//! ```bash
//! SSI_CONSUMER_ID=... SSI_CONSUMER_SECRET=... \
//!     cargo run -p trading-bot-apps --bin market_stream_ssi
//! ```
//! Optional: `SSI_DATA_URL`, `SSI_STREAM_URL` to override the default servers.

use futures::StreamExt;
use tracing::{info, warn};
use trading_bot_instrument::instrument::market_data::kind::MarketDataInstrumentKind;
use trading_bot_market::{
    exchange::ssi::Ssi,
    streams::{Streams, reconnect::stream::ReconnectingStream},
    subscription::{book::OrderBooksL1, trade::PublicTrades},
};
use trading_bot_runtime::logging::init_logging;

const SYMBOLS: [&str; 4] = ["vnm", "hpg", "fpt", "shb"];

#[tokio::main]
async fn main() {
    init_logging();

    // Each subscribe() batch opens one SignalR connection
    let trades = Streams::<PublicTrades>::builder()
        .subscribe(SYMBOLS.map(|symbol| {
            (
                Ssi,
                symbol,
                "vnd",
                MarketDataInstrumentKind::Spot,
                PublicTrades,
            )
        }))
        .init()
        .await
        .expect("failed to initialise SSI PublicTrades stream");

    let quotes = Streams::<OrderBooksL1>::builder()
        .subscribe(SYMBOLS.map(|symbol| {
            (
                Ssi,
                symbol,
                "vnd",
                MarketDataInstrumentKind::Spot,
                OrderBooksL1,
            )
        }))
        .init()
        .await
        .expect("failed to initialise SSI OrderBooksL1 stream");

    let mut trades = trades
        .select_all()
        .with_error_handler(|error| warn!(?error, "SSI trade stream error"));
    let mut quotes = quotes
        .select_all()
        .with_error_handler(|error| warn!(?error, "SSI quote stream error"));

    loop {
        tokio::select! {
            Some(event) = trades.next() => info!(?event, "trade"),
            Some(event) = quotes.next() => info!(?event, "quote"),
            else => break,
        }
    }
}
