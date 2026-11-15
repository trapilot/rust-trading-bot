//! End-to-end test of the TCBS price board integration against a local mock server that speaks
//! the documented wire protocol (auth -> subscribe -> ack -> data, with text heartbeats).

use futures::{SinkExt, StreamExt};
use std::time::Duration;
use tokio::{net::TcpListener, sync::mpsc};
use tokio_tungstenite::{accept_async, tungstenite::Message};
use trading_bot_instrument::{Side, instrument::market_data::kind::MarketDataInstrumentKind};
use trading_bot_market::{
    exchange::tcbs::Tcbs,
    streams::{Streams, reconnect::Event},
    subscription::trade::PublicTrades,
};

// Silence unused dev-dependency lints for this test crate
use rust_decimal_macros as _;
use tracing_subscriber as _;

const TOKEN: &str = "test-token";
// base64("test-token")
const AUTH_FRAME: &str = "d|a|||dGVzdC10b2tlbg==";

#[tokio::test]
async fn test_tcbs_public_trades_against_mock_server() {
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let address = listener.local_addr().unwrap();
    let (received_tx, mut received_rx) = mpsc::unbounded_channel::<String>();

    tokio::spawn(async move {
        let (stream, _) = listener.accept().await.unwrap();
        let mut ws = accept_async(stream).await.unwrap();

        while let Some(Ok(message)) = ws.next().await {
            let Message::Text(text) = message else {
                continue;
            };
            let text = text.to_string();
            received_tx.send(text.clone()).unwrap();

            if text.starts_with("d|a|||") {
                ws.send(Message::text("d|33|15")).await.unwrap();
                let response = if text == AUTH_FRAME {
                    r#"d|0|{"success":true,"error":null}"#
                } else {
                    r#"d|0|{"success":false,"error":{"code":"211110","message":"Invalid JWT"}}"#
                };
                ws.send(Message::text(response)).await.unwrap();
            } else if text.starts_with("d|s|tk|") {
                for frame in [
                    "d|34|1",
                    r#"s|1|{"symbol":"TCB","bidPrice01":"24550","bidPrice02":"24500","bidPrice03":"24450","bidQtty01":"137200","bidQtty02":"354000","bidQtty03":"292900"}"#,
                    r#"s|2|{"symbol":"TCB","offerPrice01":"24600","offerPrice02":"24650","offerPrice03":"24700","offerQtty01":"294000","offerQtty02":"163400","offerQtty03":"743600"}"#,
                    r#"s|6|{"symbol":"TCB","matchPrice":"24600","matchQtty":"5000","change":250.0,"changePercent":1.03,"totalVolume":"9745500","totalValue":"240774655000"}"#,
                ] {
                    ws.send(Message::text(frame)).await.unwrap();
                }
            }
        }
    });

    // SAFETY: this integration test binary runs this single test, so no other thread reads the
    // environment concurrently.
    unsafe {
        std::env::set_var("TCBS_TOKEN", TOKEN);
        std::env::set_var("TCBS_STREAM_URL", format!("ws://{address}"));
    }

    let streams = Streams::<PublicTrades>::builder()
        .subscribe([(
            Tcbs,
            "tcb",
            "vnd",
            MarketDataInstrumentKind::Spot,
            PublicTrades,
        )])
        .init()
        .await
        .expect("TCBS stream should initialise against the mock server");
    let mut stream = streams.select_all();

    // Client must authenticate first, then subscribe to trades + book sides for side inference
    assert_eq!(received_rx.recv().await.unwrap(), AUTH_FRAME);
    assert_eq!(received_rx.recv().await.unwrap(), "d|s|tk|tm+bi+op|TCB");

    // First trade: matched at the best offer -> buyer initiated
    let event = tokio::time::timeout(Duration::from_secs(5), stream.next())
        .await
        .expect("expected a trade from the mock server")
        .unwrap();
    let Event::Item(Ok(trade)) = event else {
        panic!("expected a trade event, got {event:?}");
    };
    assert_eq!(
        trade.exchange,
        trading_bot_instrument::exchange::ExchangeId::Tcbs
    );
    assert_eq!(trade.kind.id, "9745500");
    assert_eq!(trade.kind.price, 24600.0);
    assert_eq!(trade.kind.amount, 5000.0);
    assert_eq!(trade.kind.side, Side::Buy);

    // Client keeps the connection alive with TCBS text heartbeats
    let heartbeat = tokio::time::timeout(Duration::from_secs(5), received_rx.recv())
        .await
        .expect("expected a heartbeat within 5s")
        .unwrap();
    assert_eq!(heartbeat, "d|p|||");
}
