use crossterm::terminal::Clear;
use crossterm::terminal::ClearType;
use crossterm::{
    execute,
    style::{Color, Print, SetForegroundColor},
};
use futures::{SinkExt, StreamExt};
use rust_decimal::Decimal;
use serde::Deserialize;
use serde::Serialize;
use std::collections::BTreeMap;
use std::io::stdout;
use std::sync::Arc;
use tokio::sync::Mutex;
use tokio_tungstenite::connect_async;
use tokio_tungstenite::tungstenite::protocol::Message;

const API_BASE: &str = "https://api.backpack.exchange/api/v1";
const WSS_URL: &str = "wss://ws.backpack.exchange";
const SYMBOL: &str = "SOL_USDC";

type Price = Decimal;
type Quantity = Decimal;

#[derive(Debug, Deserialize)]
pub struct OrderBook {
    pub asks: BTreeMap<Decimal, Decimal>,
    pub bids: BTreeMap<Decimal, Decimal>,
    pub last_update_id: u64,
}

#[derive(Debug, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct OrderBookSnapshot {
    pub asks: Vec<(Decimal, Decimal)>,
    pub bids: Vec<(Decimal, Decimal)>,
    pub last_update_id: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DepthMessage {
    data: DepthEvent,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct DepthEvent {
    /// The type of event.
    #[serde(rename(deserialize = "e"))]
    event_type: String,
    /// The time the event was emitted.
    #[serde(rename(deserialize = "E"))]
    event_time: i64,
    /// The market symbol.
    #[serde(rename(deserialize = "s"))]
    symbol: String,
    /// Changes to the depth on the asks side.
    #[serde(rename(deserialize = "a"))]
    pub asks: Vec<(Price, Quantity)>,
    /// Changes to the depth on the bids side.
    #[serde(rename(deserialize = "b"))]
    pub bids: Vec<(Price, Quantity)>,
    /// The first id of the aggregated events.
    #[serde(rename(deserialize = "U"))]
    pub first_update_id: u64,
    /// The last id of the aggregated events.
    #[serde(rename(deserialize = "u"))]
    pub last_update_id: u64,
}

#[tokio::main]
async fn main() {
    let (stream, _) = connect_async(WSS_URL).await.expect("Failed to connect");
    let (mut write, read) = stream.split();

    // Subscribe to the depth stream.
    let subscribe_message = serde_json::json!({
        "method": "SUBSCRIBE",
        "params": [format!("depth.{SYMBOL}")]
    })
    .to_string();

    write.send(Message::text(subscribe_message)).await.unwrap();

    let order_book = Arc::new(Mutex::new(OrderBook {
        asks: BTreeMap::new(),
        bids: BTreeMap::new(),
        last_update_id: 0,
    }));
    let events = Arc::new(Mutex::new(Vec::new()));

    let read_future = read.for_each(|message| async {
        let data = message.unwrap().into_data();
        let Ok(message) = serde_json::from_slice::<DepthMessage>(&data) else {
            println!("Error parsing message");
            return;
        };
        events.lock().await.push(message.data);
    });

    tokio::spawn({
        let order_book = order_book.clone();
        let events = events.clone();

        println!("Bootstrapping order book...");

        async move {
            // Bootstrap the order book. This is performed on startup.
            //
            // 1. Get the last update id from the depth stream.
            // 2. Get the snapshot of the order book from the REST API.
            // 3. Drop any events with a lower update id than the snapshot.
            // 4. Process the remaining events.
            loop {
                std::thread::sleep(std::time::Duration::from_secs(2));

                let Some(last_ws_update_id) = events
                    .lock()
                    .await
                    .first()
                    .map(|event| event.last_update_id)
                else {
                    // No depth events yet.
                    continue;
                };

                // Get the snapshot of the order book.
                let Ok(request) = reqwest::get(format!("{API_BASE}/depth?symbol={SYMBOL}")).await
                else {
                    println!("Error fetching order book snapshot");
                    continue;
                };
                let Ok(snapshot) = request.json::<OrderBookSnapshot>().await else {
                    println!("Error parsing order book snapshot");
                    continue;
                };
                let Ok(snapshot_last_update_id) = snapshot.last_update_id.parse::<u64>() else {
                    println!("Error parsing order book snapshot last update id");
                    continue;
                };

                // Check if we can use the snapshot.
                if snapshot_last_update_id >= last_ws_update_id {
                    events
                        .lock()
                        .await
                        .retain(|event| event.last_update_id > snapshot_last_update_id);

                    *order_book.lock().await = OrderBook {
                        asks: snapshot.asks.into_iter().collect(),
                        bids: snapshot.bids.into_iter().collect(),
                        last_update_id: snapshot_last_update_id,
                    };
                    break;
                }
            }

            // Process the events.
            loop {
                for event in events.lock().await.drain(..) {
                    let mut order_book = order_book.lock().await;

                    let time = chrono::Utc::now().timestamp_millis();

                    let time_diff = time - event.event_time;

                    if time_diff > 1000 {
                        println!("Time difference is too large: {}ms", time_diff);
                    } else {
                        println!("Time difference is: {}ms", time_diff);
                    }

                    if order_book.last_update_id != event.first_update_id - 1 {
                        panic!("Dropped messages");
                    }

                    order_book.last_update_id = event.last_update_id;

                    for (price, quantity) in event.asks {
                        if quantity == Decimal::ZERO {
                            order_book.asks.remove(&price);
                        } else {
                            order_book.asks.insert(price, quantity);
                        }
                    }

                    for (price, quantity) in event.bids {
                        if quantity == Decimal::ZERO {
                            order_book.bids.remove(&price);
                        } else {
                            order_book.bids.insert(price, quantity);
                        }
                    }

                    // Print the order book.
                    let _ = execute!(stdout(), Clear(ClearType::All));
                    execute!(stdout(), SetForegroundColor(Color::Red)).unwrap();
                    execute!(stdout(), Print("Asks\n")).unwrap();

                    for (price, quantity) in order_book.asks.iter().take(10) {
                        execute!(stdout(), Print(format!("{} {}\n", price, quantity))).unwrap();
                    }

                    execute!(stdout(), Print("\n")).unwrap();
                    execute!(stdout(), SetForegroundColor(Color::Green)).unwrap();
                    execute!(stdout(), Print("Bids\n")).unwrap();

                    for (price, quantity) in order_book.bids.iter().rev().take(10) {
                        execute!(stdout(), Print(format!("{} {}\n", price, quantity))).unwrap();
                    }
                }
            }
        }
    });

    read_future.await;
}
