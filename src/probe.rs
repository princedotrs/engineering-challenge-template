//! Latency experiments (Extension 4).
//!
//! Opens several independent connections to the exchange, subscribes them all
//! to the same symbols, and compares them: handshake time, which remote IP each
//! landed on, receive latency against the exchange timestamp, and how often each
//! connection is the *first* to deliver a given update. Optionally pins each
//! connection's local (source) TCP port, to test whether that changes the path
//! or server a connection gets.

use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::Duration;

use anyhow::Context;
use chrono::{DateTime, Utc};
use futures_util::{SinkExt, StreamExt};
use tokio::net::{TcpSocket, lookup_host};
use tokio::sync::mpsc;
use tokio::time::Instant;
use tokio_tungstenite::tungstenite::Message;

use crate::book::{LevelChange, Side};
use crate::exchange::{Codec, Event, Exchange, Keepalive};

#[derive(Clone, Debug)]
pub struct ProbeConfig {
    pub connections: usize,
    pub symbols: usize,
    pub duration: Duration,
    /// Bind connection `i` to local port `source_port_base + i`.
    pub source_port_base: Option<u16>,
}

struct Arrival {
    conn: usize,
    received_at: DateTime<Utc>,
    key: UpdateKey,
    exchange_ts: Option<DateTime<Utc>>,
}

#[derive(Clone, PartialEq, Eq, Hash)]
struct UpdateKey {
    symbol: String,
    side: bool,
    price: i64,
    change: i64,
    absolute: bool,
    exchange_ts: Option<i64>,
}

#[derive(Default)]
struct ConnReport {
    local: Option<SocketAddr>,
    remote: Option<SocketAddr>,
    handshake: Option<Duration>,
    error: Option<String>,
    updates: u64,
    latencies_ms: Vec<f64>,
    wins: u64,
    behind_ms: Vec<f64>,
}

fn percentile(values: &mut [f64], p: f64) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    values.sort_by(f64::total_cmp);
    let idx = ((values.len() - 1) as f64 * p).round() as usize;
    Some(values[idx])
}

async fn run_connection<E: Exchange>(
    exchange: Arc<E>,
    conn: usize,
    symbols: Vec<String>,
    config: ProbeConfig,
    arrivals: mpsc::UnboundedSender<Arrival>,
) -> ConnReport {
    let mut report = ConnReport::default();
    if let Err(error) =
        probe_connection(&*exchange, conn, &symbols, &config, &arrivals, &mut report).await
    {
        report.error = Some(format!("{error:#}"));
    }
    report
}

async fn probe_connection<E: Exchange>(
    exchange: &E,
    conn: usize,
    symbols: &[String],
    config: &ProbeConfig,
    arrivals: &mpsc::UnboundedSender<Arrival>,
    report: &mut ConnReport,
) -> anyhow::Result<()> {
    let request = exchange.connect_request()?;
    let host = request.uri().host().context("url has no host")?.to_owned();
    let port = request
        .uri()
        .port_u16()
        .unwrap_or(match request.uri().scheme_str() {
            Some("ws") => 80,
            _ => 443,
        });

    let started = Instant::now();
    let remote = lookup_host((host.as_str(), port))
        .await?
        .next()
        .context("host did not resolve")?;
    let socket = if remote.is_ipv4() {
        TcpSocket::new_v4()?
    } else {
        TcpSocket::new_v6()?
    };
    if let Some(base) = config.source_port_base {
        let local: SocketAddr = if remote.is_ipv4() {
            ([0, 0, 0, 0], base + conn as u16).into()
        } else {
            ("::".parse::<std::net::Ipv6Addr>()?, base + conn as u16).into()
        };
        socket.set_reuseaddr(true)?;
        socket.bind(local)?;
    }
    socket.set_nodelay(true)?;
    let tcp = socket.connect(remote).await?;
    tcp.set_nodelay(true)?;
    report.local = tcp.local_addr().ok();
    report.remote = Some(remote);
    let (socket, _) = tokio_tungstenite::client_async_tls(request, tcp).await?;
    report.handshake = Some(started.elapsed());

    let (mut sink, mut stream) = socket.split();
    let mut codec = exchange.new_codec();
    for message in codec.subscribe(symbols) {
        sink.send(Message::text(message)).await?;
    }

    let deadline = Instant::now() + config.duration;
    let mut keepalive = tokio::time::interval(Duration::from_secs(10));
    let mut events = Vec::new();
    loop {
        tokio::select! {
            _ = tokio::time::sleep_until(deadline) => break,
            _ = keepalive.tick() => {
                let message = match codec.keepalive() {
                    Keepalive::Text(text) => Message::text(text),
                    Keepalive::PingFrame => Message::Ping(Default::default()),
                };
                sink.send(message).await?;
            }
            frame = stream.next() => {
                let received_at = Utc::now();
                let Some(frame) = frame else { anyhow::bail!("stream ended") };
                let Message::Text(text) = frame? else { continue };
                let _ = codec.decode(&text, &mut events);
                for event in events.drain(..) {
                    let Event::Delta { symbol, side, price, change, exchange_ts } = event else {
                        continue;
                    };
                    let (change, absolute) = match change {
                        LevelChange::Set(qty) => (qty, true),
                        LevelChange::Add(delta) => (delta, false),
                    };
                    let key = UpdateKey {
                        symbol,
                        side: side == Side::Bid,
                        price,
                        change,
                        absolute,
                        exchange_ts: exchange_ts.map(|ts| ts.timestamp_micros()),
                    };
                    let _ = arrivals.send(Arrival { conn, received_at, key, exchange_ts });
                }
            }
        }
    }
    let _ = sink.send(Message::Close(None)).await;
    Ok(())
}

pub async fn run<E: Exchange>(exchange: E, config: ProbeConfig) -> anyhow::Result<()> {
    eprintln!("discovering symbols...");
    let mut symbols = exchange.discover().await?.symbols;
    symbols.truncate(config.symbols);
    anyhow::ensure!(!symbols.is_empty(), "no symbols to probe");
    eprintln!(
        "probing {} connections x {} symbols for {:?}",
        config.connections,
        symbols.len(),
        config.duration
    );

    let exchange = Arc::new(exchange);
    let (tx, mut rx) = mpsc::unbounded_channel();
    let tasks: Vec<_> = (0..config.connections)
        .map(|conn| {
            tokio::spawn(run_connection(
                exchange.clone(),
                conn,
                symbols.clone(),
                config.clone(),
                tx.clone(),
            ))
        })
        .collect();
    drop(tx);

    // First arrival of each distinct update, across all connections.
    let mut first: HashMap<UpdateKey, DateTime<Utc>> = HashMap::new();
    let mut arrivals = Vec::new();
    while let Some(arrival) = rx.recv().await {
        let entry = first
            .entry(arrival.key.clone())
            .or_insert(arrival.received_at);
        if arrival.received_at < *entry {
            *entry = arrival.received_at;
        }
        arrivals.push(arrival);
    }

    let mut reports = Vec::new();
    for task in tasks {
        reports.push(task.await?);
    }
    for arrival in &arrivals {
        let report = &mut reports[arrival.conn];
        report.updates += 1;
        if let Some(ts) = arrival.exchange_ts {
            report
                .latencies_ms
                .push((arrival.received_at - ts).num_microseconds().unwrap_or(0) as f64 / 1000.0);
        }
        let winner = first[&arrival.key];
        let behind = (arrival.received_at - winner)
            .num_microseconds()
            .unwrap_or(0) as f64
            / 1000.0;
        if behind == 0.0 {
            report.wins += 1;
        }
        report.behind_ms.push(behind);
    }

    println!(
        "{:>4}  {:<22} {:<22} {:>10} {:>8} {:>10} {:>10} {:>7} {:>11} {:>11}",
        "conn",
        "local",
        "remote",
        "handshake",
        "updates",
        "p50 lat",
        "p99 lat",
        "wins",
        "p50 behind",
        "p99 behind"
    );
    let fmt = |v: Option<f64>| v.map_or("-".to_owned(), |v| format!("{v:.1}ms"));
    for (conn, report) in reports.iter_mut().enumerate() {
        if let Some(error) = &report.error {
            println!("{conn:>4}  error: {error}");
            continue;
        }
        let wins = if report.updates == 0 {
            "-".to_owned()
        } else {
            format!("{:.0}%", 100.0 * report.wins as f64 / report.updates as f64)
        };
        println!(
            "{:>4}  {:<22} {:<22} {:>10} {:>8} {:>10} {:>10} {:>7} {:>11} {:>11}",
            conn,
            report.local.map_or("-".into(), |a| a.to_string()),
            report.remote.map_or("-".into(), |a| a.to_string()),
            report
                .handshake
                .map_or("-".into(), |d| format!("{:.0}ms", d.as_secs_f64() * 1000.0)),
            report.updates,
            fmt(percentile(&mut report.latencies_ms, 0.5)),
            fmt(percentile(&mut report.latencies_ms, 0.99)),
            wins,
            fmt(percentile(&mut report.behind_ms, 0.5)),
            fmt(percentile(&mut report.behind_ms, 0.99)),
        );
    }
    println!(
        "\n{} distinct updates; latency = receive time - exchange timestamp (includes clock offset); \
         behind = time after the fastest connection delivered the same update",
        first.len()
    );
    Ok(())
}
