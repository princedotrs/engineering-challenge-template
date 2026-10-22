//! End-to-end test against a local mock exchange that speaks the Polymarket
//! market-channel protocol.
//!
//! The mock keeps the true state of every book. The test drops a connection,
//! silently mutes another one, and removes a symbol mid-run, then checks that
//! replaying the collector's output reproduces the true books exactly.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use engineering_challenge::book::OrderBook;
use engineering_challenge::collector::{self, CollectorConfig};
use engineering_challenge::connection::ConnConfig;
use engineering_challenge::engine::EngineConfig;
use engineering_challenge::exchange::polymarket::PolymarketCodec;
use engineering_challenge::exchange::{Discovered, Exchange};
use engineering_challenge::output::Target;
use engineering_challenge::supervisor::SupervisorConfig;
use futures_util::{SinkExt, StreamExt};
use serde_json::{Value, json};
use tokio::net::TcpListener;
use tokio::sync::{broadcast, oneshot};
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;

type Levels = BTreeMap<i64, i64>;

fn decimal(value: i64) -> String {
    format!("{}.{:06}", value / 1_000_000, value % 1_000_000)
}

#[derive(Default)]
struct ConnInfo {
    muted: Arc<AtomicBool>,
    subscribes: Arc<AtomicUsize>,
    unsubscribed: Arc<Mutex<Vec<String>>>,
    kill: Mutex<Option<oneshot::Sender<()>>>,
}

#[derive(Default)]
struct ServerState {
    books: HashMap<String, (Levels, Levels)>,
    conns: Vec<Arc<ConnInfo>>,
}

#[derive(Clone)]
struct MockServer {
    url: String,
    state: Arc<Mutex<ServerState>>,
    frames: broadcast::Sender<(String, String)>,
}

impl MockServer {
    async fn start(symbols: &[&str]) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("ws://{}", listener.local_addr().unwrap());
        let mut state = ServerState::default();
        for symbol in symbols {
            state.books.insert(
                symbol.to_string(),
                (
                    BTreeMap::from([(400_000, 10_000_000), (450_000, 5_000_000)]),
                    BTreeMap::from([(550_000, 5_000_000), (600_000, 10_000_000)]),
                ),
            );
        }
        let (frames, _) = broadcast::channel(10_000);
        let server = Self {
            url,
            state: Arc::new(Mutex::new(state)),
            frames,
        };
        let accept = server.clone();
        tokio::spawn(async move {
            while let Ok((stream, _)) = listener.accept().await {
                let server = accept.clone();
                tokio::spawn(async move { server.serve(stream).await });
            }
        });
        server
    }

    fn book_frame(&self, symbols: &[String]) -> String {
        let state = self.state.lock().unwrap();
        let books: Vec<Value> = symbols
            .iter()
            .filter_map(|symbol| {
                let (bids, asks) = state.books.get(symbol)?;
                let levels = |levels: &Levels| {
                    levels
                        .iter()
                        .map(|(p, q)| json!({"price": decimal(*p), "size": decimal(*q)}))
                        .collect::<Vec<_>>()
                };
                Some(json!({
                    "event_type": "book", "asset_id": symbol, "market": "0x1",
                    "bids": levels(bids), "asks": levels(asks),
                    "timestamp": chrono::Utc::now().timestamp_millis().to_string(),
                }))
            })
            .collect();
        Value::Array(books).to_string()
    }

    async fn serve(self, stream: tokio::net::TcpStream) {
        let mut socket = tokio_tungstenite::accept_async(stream).await.unwrap();
        let info = Arc::new(ConnInfo::default());
        let (kill_tx, mut kill_rx) = oneshot::channel();
        *info.kill.lock().unwrap() = Some(kill_tx);
        self.state.lock().unwrap().conns.push(info.clone());
        let mut frames = self.frames.subscribe();
        let mut subscribed: HashSet<String> = HashSet::new();

        loop {
            tokio::select! {
                message = socket.next() => {
                    let Some(Ok(Message::Text(text))) = message else { return };
                    if text.as_str() == "PING" {
                        let _ = socket.send(Message::text("PONG")).await;
                        continue;
                    }
                    let request: Value = serde_json::from_str(&text).unwrap();
                    let assets: Vec<String> =
                        serde_json::from_value(request["assets_ids"].clone()).unwrap();
                    if request["operation"] == "unsubscribe" {
                        for asset in &assets {
                            subscribed.remove(asset);
                        }
                        info.unsubscribed.lock().unwrap().extend(assets);
                    } else {
                        subscribed.extend(assets.iter().cloned());
                        info.subscribes.fetch_add(1, Ordering::SeqCst);
                        let frame = self.book_frame(&assets);
                        let _ = socket.send(Message::text(frame)).await;
                    }
                }
                frame = frames.recv() => {
                    let Ok((asset, frame)) = frame else { return };
                    if subscribed.contains(&asset) && !info.muted.load(Ordering::SeqCst) {
                        let _ = socket.send(Message::text(frame)).await;
                    }
                }
                // Drop the TCP connection without a close frame.
                _ = &mut kill_rx => return,
            }
        }
    }

    /// Changes a level in the true book and broadcasts the price change.
    fn set_level(&self, symbol: &str, bid: bool, price: i64, size: i64) {
        let frame = {
            let mut state = self.state.lock().unwrap();
            let (bids, asks) = state.books.get_mut(symbol).unwrap();
            let levels = if bid { &mut *bids } else { &mut *asks };
            if size == 0 {
                levels.remove(&price);
            } else {
                levels.insert(price, size);
            }
            let best_bid = bids.keys().next_back().copied().unwrap_or(0);
            let best_ask = asks.keys().next().copied().unwrap_or(0);
            json!({
                "event_type": "price_change", "market": "0x1",
                "timestamp": chrono::Utc::now().timestamp_millis().to_string(),
                "price_changes": [{
                    "asset_id": symbol, "price": decimal(price), "size": decimal(size),
                    "side": if bid { "BUY" } else { "SELL" }, "hash": "0x0",
                    "best_bid": decimal(best_bid), "best_ask": decimal(best_ask),
                }],
            })
            .to_string()
        };
        let _ = self.frames.send((symbol.to_owned(), frame));
    }

    fn conn(&self, idx: usize) -> Option<Arc<ConnInfo>> {
        self.state.lock().unwrap().conns.get(idx).cloned()
    }

    fn truth(&self) -> HashMap<String, OrderBook> {
        let state = self.state.lock().unwrap();
        state
            .books
            .iter()
            .map(|(symbol, (bids, asks))| {
                let bids: Vec<_> = bids.iter().map(|(p, q)| (*p, *q)).collect();
                let asks: Vec<_> = asks.iter().map(|(p, q)| (*p, *q)).collect();
                (symbol.clone(), OrderBook::from_levels(&bids, &asks))
            })
            .collect()
    }
}

struct MockExchange {
    url: String,
    symbols: Arc<Mutex<Vec<String>>>,
}

impl Exchange for MockExchange {
    type Codec = PolymarketCodec;

    fn name(&self) -> &'static str {
        "mock"
    }

    async fn discover(&self) -> anyhow::Result<Discovered> {
        Ok(Discovered::complete(self.symbols.lock().unwrap().clone()))
    }

    fn connect_request(&self) -> anyhow::Result<Request> {
        Ok(self.url.as_str().into_client_request()?)
    }

    fn new_codec(&self) -> PolymarketCodec {
        PolymarketCodec::new(true)
    }
}

async fn wait_for(what: &str, mut condition: impl FnMut() -> bool) {
    for _ in 0..200 {
        if condition() {
            return;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    panic!("timed out waiting for {what}");
}

/// Applies a burst of level changes across the given symbols.
async fn churn(server: &MockServer, symbols: &[&str], rounds: i64, offset: i64) {
    for round in 0..rounds {
        for symbol in symbols {
            let n = round + offset;
            server.set_level(
                symbol,
                true,
                300_000 + (n % 5) * 10_000,
                (n % 3) * 1_000_000,
            );
            server.set_level(
                symbol,
                false,
                650_000 + (n % 4) * 10_000,
                (n % 2) * 2_000_000,
            );
        }
        tokio::time::sleep(Duration::from_millis(20)).await;
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn survives_disconnects_silent_subscriptions_and_removals() {
    let server = MockServer::start(&["1", "2", "3"]).await;
    let symbols = Arc::new(Mutex::new(vec!["1".into(), "2".into(), "3".into()]));
    let exchange = MockExchange {
        url: server.url.clone(),
        symbols: symbols.clone(),
    };
    let output = tempfile::NamedTempFile::new().unwrap();
    let config = CollectorConfig {
        target: Target::File(output.path().to_owned()),
        engine: EngineConfig {
            snapshot_interval: Duration::from_secs(1),
            resubscribe_backoff: Duration::from_millis(300),
            resync_timeout: Duration::from_secs(2),
            replica_silence: Duration::from_millis(800),
            stats_interval: Duration::from_secs(5),
            ..EngineConfig::default()
        },
        supervisor: SupervisorConfig {
            replicas: 2,
            symbols_per_connection: 10,
            discovery_interval: Duration::from_millis(200),
            removal_confirmations: 2,
            connects_per_second: 100.0,
            max_symbols: None,
            connection: ConnConfig {
                keepalive_interval: Duration::from_millis(500),
                idle_timeout: Duration::from_secs(5),
                recycle_interval: None,
                ..ConnConfig::default()
            },
        },
    };
    let (stop_tx, stop_rx) = oneshot::channel::<()>();
    let collector = tokio::spawn(collector::run(exchange, config, async {
        let _ = stop_rx.await;
    }));

    wait_for("both replicas to subscribe", || {
        (0..2).all(|i| {
            server
                .conn(i)
                .is_some_and(|c| c.subscribes.load(Ordering::SeqCst) > 0)
        })
    })
    .await;
    tokio::time::sleep(Duration::from_millis(200)).await;
    churn(&server, &["1", "2", "3"], 20, 0).await;

    // 1. Drop a connection without warning; keep trading while it reconnects.
    server
        .conn(0)
        .unwrap()
        .kill
        .lock()
        .unwrap()
        .take()
        .unwrap()
        .send(())
        .unwrap();
    churn(&server, &["1", "2", "3"], 20, 100).await;
    wait_for("the dropped replica to come back", || {
        server
            .conn(2)
            .is_some_and(|c| c.subscribes.load(Ordering::SeqCst) > 0)
    })
    .await;
    churn(&server, &["1", "2", "3"], 10, 200).await;

    // 2. A subscription silently stops delivering data.
    let muted = server.conn(1).unwrap();
    muted.muted.store(true, Ordering::SeqCst);
    let before = muted.subscribes.load(Ordering::SeqCst);
    for round in 0..200 {
        churn(&server, &["1", "2"], 1, 300 + round).await;
        if muted.subscribes.load(Ordering::SeqCst) > before {
            break;
        }
    }
    assert!(
        muted.subscribes.load(Ordering::SeqCst) > before,
        "silent subscription was never resubscribed"
    );
    muted.muted.store(false, Ordering::SeqCst);
    churn(&server, &["1", "2", "3"], 10, 600).await;

    // 3. Symbol 3 stops trading.
    symbols.lock().unwrap().retain(|s| s != "3");
    wait_for("symbol 3 to be unsubscribed", || {
        let conns = server.state.lock().unwrap().conns.clone();
        conns
            .iter()
            .any(|c| c.unsubscribed.lock().unwrap().contains(&"3".to_owned()))
    })
    .await;
    churn(&server, &["1", "2"], 10, 700).await;
    tokio::time::sleep(Duration::from_millis(500)).await;

    stop_tx.send(()).unwrap();
    let summary = tokio::time::timeout(Duration::from_secs(20), collector)
        .await
        .expect("collector shut down in time")
        .unwrap()
        .unwrap();
    assert!(summary.engine.source_switches >= 1, "{summary:?}");

    let replay =
        engineering_challenge::dump::dump(output.path(), true, &mut std::io::sink()).unwrap();
    eprintln!(
        "engine: {:?}\nreplay: snapshots={} updates={}",
        summary.engine, replay.snapshots, replay.updates
    );
    assert_eq!(replay.updates_without_snapshot, 0);
    assert_eq!(replay.crossed_after_update, 0);
    let truth = server.truth();
    for symbol in ["1", "2", "3"] {
        assert_eq!(
            replay.books.get(symbol),
            truth.get(symbol),
            "replayed book for {symbol} differs from the exchange"
        );
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn probe_compares_connections() {
    let server = MockServer::start(&["1", "2"]).await;
    let exchange = MockExchange {
        url: server.url.clone(),
        symbols: Arc::new(Mutex::new(vec!["1".into(), "2".into()])),
    };
    let config = engineering_challenge::probe::ProbeConfig {
        connections: 3,
        symbols: 2,
        duration: Duration::from_secs(2),
        source_port_base: None,
    };
    let probe = tokio::spawn(engineering_challenge::probe::run(exchange, config));
    wait_for("probe connections", || {
        (0..3).all(|i| {
            server
                .conn(i)
                .is_some_and(|c| c.subscribes.load(Ordering::SeqCst) > 0)
        })
    })
    .await;
    churn(&server, &["1", "2"], 20, 0).await;
    probe.await.unwrap().unwrap();
}
