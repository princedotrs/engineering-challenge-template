//! One websocket connection to the exchange.
//!
//! The task owns its symbol set and keeps it subscribed across reconnects. It
//! reconnects with jittered exponential backoff, treats a socket that has been
//! silent for too long as dead, and periodically recycles itself (Extension 2)
//! to shed whatever state builds up on either side of a long-lived connection.

use std::collections::BTreeSet;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::Duration;

use chrono::Utc;
use futures_util::{SinkExt, StreamExt};
use rand::Rng;
use tokio::sync::{mpsc, watch};
use tokio::time::{Instant, sleep, sleep_until, timeout};
use tokio_tungstenite::tungstenite::Message;
use tracing::{debug, info, warn};

use crate::engine::{ConnCommand, ConnId, EngineMsg};
use crate::exchange::{Codec, Exchange, Keepalive};

#[derive(Clone, Debug)]
pub struct ConnConfig {
    pub keepalive_interval: Duration,
    /// No frames at all for this long means the socket is dead.
    pub idle_timeout: Duration,
    /// Recycle the connection this often (`None` disables recycling).
    pub recycle_interval: Option<Duration>,
    /// Symbols per subscribe message.
    pub subscribe_batch: usize,
    pub connect_timeout: Duration,
}

impl Default for ConnConfig {
    fn default() -> Self {
        Self {
            keepalive_interval: Duration::from_secs(10),
            idle_timeout: Duration::from_secs(30),
            recycle_interval: Some(Duration::from_secs(30 * 60)),
            subscribe_batch: 100,
            connect_timeout: Duration::from_secs(15),
        }
    }
}

/// Spaces out connection attempts across all connections, so a mass reconnect
/// (startup, or an exchange-wide disconnect) doesn't trip connection limits.
pub struct ConnectLimiter {
    next: Mutex<Instant>,
    gap: Duration,
}

impl ConnectLimiter {
    pub fn new(per_second: f64) -> Self {
        Self {
            next: Mutex::new(Instant::now()),
            gap: Duration::from_secs_f64(1.0 / per_second.max(0.01)),
        }
    }

    pub async fn wait(&self) {
        let at = {
            let mut next = self.next.lock().unwrap_or_else(|e| e.into_inner());
            let at = (*next).max(Instant::now());
            *next = at + self.gap;
            at
        };
        sleep_until(at).await;
    }
}

pub struct Connection<E: Exchange> {
    pub id: ConnId,
    pub exchange: Arc<E>,
    pub symbols: BTreeSet<String>,
    pub commands: mpsc::UnboundedReceiver<ConnCommand>,
    pub engine: mpsc::UnboundedSender<EngineMsg>,
    pub limiter: Arc<ConnectLimiter>,
    pub config: ConnConfig,
    pub shutdown: watch::Receiver<bool>,
    /// When to recycle next. Replicas of a shard get fixed, evenly spaced
    /// phases, so they never come due together.
    pub next_recycle: Option<Instant>,
    /// Connected replicas in this shard, shared by its connections.
    pub shard_up: Arc<AtomicUsize>,
    pub replicas: usize,
}

/// How long a connection must stay up before the reconnect backoff resets.
const STABLE_AFTER: Duration = Duration::from_secs(30);
/// How long to wait before recycling while a sibling replica is down.
const RECYCLE_POSTPONE: Duration = Duration::from_secs(30);
/// How long to keep reading after sending our close on shutdown.
const SHUTDOWN_DRAIN: Duration = Duration::from_secs(1);

enum Exit {
    Shutdown,
    Recycle,
    Lost(String),
}

fn jitter(duration: Duration, fraction: f64) -> Duration {
    let factor = 1.0 + rand::thread_rng().gen_range(-fraction..=fraction);
    duration.mul_f64(factor.max(0.0))
}

impl<E: Exchange> Connection<E> {
    fn apply_bookkeeping(&mut self, command: &ConnCommand) {
        match command {
            ConnCommand::Subscribe(symbols) => self.symbols.extend(symbols.iter().cloned()),
            ConnCommand::Unsubscribe(symbols) => {
                for symbol in symbols {
                    self.symbols.remove(symbol);
                }
            }
            ConnCommand::Resubscribe(_) => {}
        }
    }

    pub async fn run(mut self) {
        let mut backoff = Duration::from_secs(1);
        loop {
            if *self.shutdown.borrow() {
                break;
            }
            // Wait for a connect slot, still tracking symbol changes.
            let limiter = self.limiter.clone();
            let wait = limiter.wait();
            tokio::pin!(wait);
            loop {
                tokio::select! {
                    _ = &mut wait => break,
                    Some(command) = self.commands.recv() => self.apply_bookkeeping(&command),
                    _ = self.shutdown.changed() => return,
                }
            }

            let attempt = Instant::now();
            let exit = self
                .connect_and_stream()
                .await
                .unwrap_or_else(|error| Exit::Lost(format!("{error:#}")));
            // Only a connection that stayed up resets the backoff, so a server
            // that accepts and immediately drops us isn't hammered.
            if attempt.elapsed() >= STABLE_AFTER {
                backoff = Duration::from_secs(1);
            }
            match exit {
                Exit::Shutdown => break,
                Exit::Recycle => {
                    info!(conn = self.id, "recycling connection");
                    continue;
                }
                Exit::Lost(reason) => {
                    warn!(conn = self.id, %reason, retry_in = ?backoff, "connection lost");
                }
            }

            let wake = Instant::now() + jitter(backoff, 0.25);
            backoff = (backoff * 2).min(Duration::from_secs(60));
            loop {
                tokio::select! {
                    _ = sleep_until(wake) => break,
                    Some(command) = self.commands.recv() => self.apply_bookkeeping(&command),
                    _ = self.shutdown.changed() => return,
                }
            }
        }
    }

    async fn connect_and_stream(&mut self) -> anyhow::Result<Exit> {
        let request = self.exchange.connect_request()?;
        let (socket, _) = timeout(
            self.config.connect_timeout,
            tokio_tungstenite::connect_async_with_config(request, None, true),
        )
        .await
        .map_err(|_| anyhow::anyhow!("connect timed out"))??;
        let (mut sink, mut stream) = socket.split();
        let mut codec = self.exchange.new_codec();

        debug!(conn = self.id, symbols = self.symbols.len(), "connected");
        let _ = self.engine.send(EngineMsg::ConnUp(self.id));
        self.shard_up.fetch_add(1, Ordering::SeqCst);
        let exit = self.stream(&mut sink, &mut stream, &mut codec).await;
        self.shard_up.fetch_sub(1, Ordering::SeqCst);
        // On shutdown every connection stops at once; failing over between
        // them would only write a burst of pointless snapshots.
        if !matches!(exit, Exit::Shutdown) {
            let _ = self.engine.send(EngineMsg::ConnDown(self.id));
        }

        // Close politely, but don't let a stuck socket hold us up.
        let _ = timeout(Duration::from_secs(2), async {
            let _ = sink.send(Message::Close(None)).await;
            let _ = sink.close().await;
        })
        .await;
        Ok(exit)
    }

    /// Decodes a text frame and forwards its events to the engine.
    fn forward(
        &self,
        codec: &mut E::Codec,
        text: &str,
        received_at: chrono::DateTime<Utc>,
        events: &mut Vec<crate::exchange::Event>,
    ) {
        if let Err(error) = codec.decode(text, events) {
            debug!(conn = self.id, %error, "undecodable frame");
        }
        if !events.is_empty() {
            let _ = self.engine.send(EngineMsg::Feed {
                conn: self.id,
                received_at,
                events: std::mem::take(events),
            });
        }
    }

    async fn stream<S, R>(&mut self, sink: &mut S, stream: &mut R, codec: &mut E::Codec) -> Exit
    where
        S: futures_util::Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
        R: futures_util::Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
            + Unpin,
    {
        let symbols: Vec<String> = self.symbols.iter().cloned().collect();
        for batch in symbols.chunks(self.config.subscribe_batch.max(1)) {
            for message in codec.subscribe(batch) {
                if let Err(error) = sink.send(Message::text(message)).await {
                    return Exit::Lost(format!("subscribe failed: {error}"));
                }
            }
        }

        // A recycle that came due while we were disconnected is moot: this is
        // a fresh connection. Move to the next slot of the schedule.
        if let (Some(next), Some(interval)) =
            (self.next_recycle.as_mut(), self.config.recycle_interval)
        {
            while *next <= Instant::now() {
                *next += interval;
            }
        }
        let mut postponed_until: Option<Instant> = None;
        let mut keepalive = tokio::time::interval(self.config.keepalive_interval);
        keepalive.reset();
        let mut last_rx = Instant::now();
        let mut events = Vec::new();

        loop {
            let recycle_at = postponed_until.or(self.next_recycle);
            tokio::select! {
                frame = stream.next() => {
                    // Timestamp as early as possible: this is our receive time.
                    let received_at = Utc::now();
                    last_rx = Instant::now();
                    match frame {
                        Some(Ok(Message::Text(text))) => {
                            self.forward(codec, &text, received_at, &mut events);
                            for message in codec.drain_outbox() {
                                if let Err(error) = sink.send(Message::text(message)).await {
                                    return Exit::Lost(format!("send failed: {error}"));
                                }
                            }
                        }
                        Some(Ok(Message::Close(frame))) => {
                            return Exit::Lost(format!("closed by exchange: {frame:?}"));
                        }
                        Some(Ok(_)) => {}
                        Some(Err(error)) => return Exit::Lost(error.to_string()),
                        None => return Exit::Lost("stream ended".into()),
                    }
                }
                command = self.commands.recv() => {
                    let Some(command) = command else { return Exit::Shutdown };
                    self.apply_bookkeeping(&command);
                    let messages = match command {
                        ConnCommand::Subscribe(symbols) => symbols
                            .chunks(self.config.subscribe_batch.max(1))
                            .flat_map(|batch| codec.subscribe(batch))
                            .collect(),
                        ConnCommand::Unsubscribe(symbols) => codec.unsubscribe(&symbols),
                        ConnCommand::Resubscribe(symbols) => {
                            let symbols: Vec<String> = symbols
                                .into_iter()
                                .filter(|s| self.symbols.contains(s))
                                .collect();
                            let mut messages = codec.unsubscribe(&symbols);
                            messages.extend(codec.subscribe(&symbols));
                            messages
                        }
                    };
                    for message in messages {
                        if let Err(error) = sink.send(Message::text(message)).await {
                            return Exit::Lost(format!("send failed: {error}"));
                        }
                    }
                }
                _ = keepalive.tick() => {
                    if last_rx.elapsed() > self.config.idle_timeout {
                        return Exit::Lost(format!("no frames for {:?}", last_rx.elapsed()));
                    }
                    let message = match codec.keepalive() {
                        Keepalive::Text(text) => Message::text(text),
                        Keepalive::PingFrame => Message::Ping(Default::default()),
                    };
                    if let Err(error) = sink.send(message).await {
                        return Exit::Lost(format!("keepalive failed: {error}"));
                    }
                }
                _ = async { sleep_until(recycle_at.unwrap()).await }, if recycle_at.is_some() => {
                    // Never recycle while a sibling is down: we'd leave the
                    // shard's symbols uncovered.
                    if self.shard_up.load(Ordering::SeqCst) < self.replicas {
                        postponed_until = Some(Instant::now() + RECYCLE_POSTPONE);
                        continue;
                    }
                    if let (Some(next), Some(interval)) =
                        (self.next_recycle.as_mut(), self.config.recycle_interval)
                    {
                        // Advance along the fixed schedule to keep the phase.
                        while *next <= Instant::now() {
                            *next += interval;
                        }
                    }
                    return Exit::Recycle;
                }
                _ = self.shutdown.changed() => {
                    // Ask the exchange to close, and keep what is already on
                    // the wire: frames received before the close still count.
                    let _ = sink.send(Message::Close(None)).await;
                    let _ = timeout(SHUTDOWN_DRAIN, async {
                        while let Some(Ok(frame)) = stream.next().await {
                            let received_at = Utc::now();
                            match frame {
                                Message::Text(text) => {
                                    self.forward(codec, &text, received_at, &mut events)
                                }
                                Message::Close(_) => break,
                                _ => {}
                            }
                        }
                    })
                    .await;
                    return Exit::Shutdown;
                }
            }
        }
    }
}

/// Sleeps for `duration` unless shutdown is requested first.
pub async fn sleep_or_shutdown(duration: Duration, shutdown: &mut watch::Receiver<bool>) -> bool {
    tokio::select! {
        _ = sleep(duration) => false,
        _ = shutdown.changed() => true,
    }
}
