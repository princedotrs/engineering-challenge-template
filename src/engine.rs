//! The engine: book building, source selection and subscription health.
//!
//! Every symbol is carried by several redundant connections ("replicas").
//! Each replica keeps its own copy of the book, built only from what arrived on
//! that connection. For every symbol exactly one replica is the *source*: only
//! its changes are written to the output. When the source becomes unusable
//! (disconnected, sequence gap, crossed or silent book, or consistently slower
//! than another replica) the engine switches to a healthy replica and writes a
//! full snapshot from that replica's book, since the two replicas' books may
//! differ.
//!
//! The engine is a plain struct driven by [`Engine::handle`] and
//! [`Engine::tick`], with time passed in, so its policies are unit-testable.

use std::collections::HashMap;
use std::time::Duration;

use chrono::{DateTime, Utc};
use crossbeam::channel::Sender;
use tokio::sync::mpsc;
use tokio::time::Instant;
use tracing::{debug, info, warn};

use crate::book::{OrderBook, Side};
use crate::exchange::Event;
use crate::output::Output;
use crate::util::dtos::challenge_orderbook::{BookSide, PriceLevelUpdateDomain};

pub type ConnId = usize;

/// Commands the engine and supervisor send to a connection task.
#[derive(Debug, PartialEq, Eq)]
pub enum ConnCommand {
    Subscribe(Vec<String>),
    Unsubscribe(Vec<String>),
    /// Unsubscribe and subscribe again to get a fresh snapshot.
    Resubscribe(Vec<String>),
}

pub enum EngineMsg {
    /// Events decoded from one websocket frame.
    Feed {
        conn: ConnId,
        received_at: DateTime<Utc>,
        events: Vec<Event>,
    },
    ConnUp(ConnId),
    ConnDown(ConnId),
    Register {
        conn: ConnId,
        commands: mpsc::UnboundedSender<ConnCommand>,
    },
    /// `symbols` are now carried by every connection in `conns`.
    AddSymbols {
        symbols: Vec<String>,
        conns: Vec<ConnId>,
    },
    RemoveSymbols(Vec<String>),
}

#[derive(Clone, Debug)]
pub struct EngineConfig {
    /// Full snapshot of every book at this wall-clock interval.
    pub snapshot_interval: Duration,
    /// Rotate the output file at this wall-clock interval (directory output).
    pub rotate_interval: Option<Duration>,
    /// Never resubscribe the same symbol on the same connection more often.
    pub resubscribe_backoff: Duration,
    /// Retry a subscription that has not produced a snapshot after this long.
    pub resync_timeout: Duration,
    /// A book that stays crossed this long is considered corrupt.
    pub crossed_timeout: Duration,
    /// A replica that has been silent this much longer than a sibling replica
    /// is missing data and gets resubscribed.
    pub replica_silence: Duration,
    /// Quiet-symbol detection: resubscribe when a symbol has been silent on
    /// every replica for `quiet_factor` × its usual gap between updates,
    /// clamped to `[quiet_min, quiet_max]`.
    pub quiet_factor: f64,
    pub quiet_min: Duration,
    pub quiet_max: Duration,
    /// Switch to a replica whose connection latency is lower by this much.
    pub latency_switch_ms: f64,
    /// Minimum time between source switches for latency reasons.
    pub min_switch_interval: Duration,
    pub stats_interval: Duration,
}

impl Default for EngineConfig {
    fn default() -> Self {
        Self {
            snapshot_interval: Duration::from_secs(60),
            rotate_interval: None,
            resubscribe_backoff: Duration::from_secs(10),
            resync_timeout: Duration::from_secs(30),
            crossed_timeout: Duration::from_secs(5),
            replica_silence: Duration::from_secs(60),
            quiet_factor: 20.0,
            quiet_min: Duration::from_secs(5 * 60),
            quiet_max: Duration::from_secs(60 * 60),
            latency_switch_ms: 50.0,
            min_switch_interval: Duration::from_secs(30),
            stats_interval: Duration::from_secs(30),
        }
    }
}

#[derive(Debug, Default, Clone)]
pub struct EngineStats {
    pub events: u64,
    pub updates_written: u64,
    pub snapshots_written: u64,
    pub source_switches: u64,
    pub resubscribes: u64,
    pub desyncs: u64,
    pub ignored: u64,
}

struct ConnState {
    up: bool,
    /// EWMA of receive time minus exchange time, in milliseconds.
    latency_ms: Option<f64>,
    commands: Option<mpsc::UnboundedSender<ConnCommand>>,
}

struct Replica {
    conn: ConnId,
    book: OrderBook,
    synced: bool,
    last_msg: Option<Instant>,
    /// Last level change (not snapshot) on this replica.
    last_delta: Option<Instant>,
    /// When the current book was received.
    synced_at: Option<Instant>,
    crossed_since: Option<Instant>,
    /// When we last (re)subscribed, including implicitly on (re)connect.
    last_subscribe: Instant,
    /// A resubscribe was wanted but held back by the backoff.
    resubscribe_pending: bool,
}

struct SymbolState {
    replicas: Vec<Replica>,
    source: Option<usize>,
    last_switch: Option<Instant>,
    /// Last book change seen on any replica.
    last_activity: Option<Instant>,
    /// EWMA of seconds between changes on the source.
    mean_gap: Option<f64>,
    /// Last change written from the source.
    last_source_delta: Option<Instant>,
    last_quiet_resubscribe: Option<Instant>,
    added_at: Instant,
}

pub struct Engine {
    config: EngineConfig,
    symbols: HashMap<String, SymbolState>,
    conns: HashMap<ConnId, ConnState>,
    output: Sender<Output>,
    stats: EngineStats,
    last_stats: EngineStats,
    next_snapshot: DateTime<Utc>,
    next_rotation: Option<DateTime<Utc>>,
    next_stats: Instant,
    next_health: Instant,
}

fn next_boundary(now: DateTime<Utc>, interval: Duration) -> DateTime<Utc> {
    let step = interval.as_millis().max(1) as i64;
    let next = (now.timestamp_millis() / step + 1) * step;
    DateTime::from_timestamp_millis(next).unwrap_or(now)
}

fn book_side(side: Side) -> BookSide {
    match side {
        Side::Bid => BookSide::Bid,
        Side::Ask => BookSide::Ask,
    }
}

impl Engine {
    pub fn new(
        config: EngineConfig,
        output: Sender<Output>,
        now: Instant,
        wall: DateTime<Utc>,
    ) -> Self {
        Self {
            next_snapshot: next_boundary(wall, config.snapshot_interval),
            next_rotation: config.rotate_interval.map(|i| next_boundary(wall, i)),
            next_stats: now + config.stats_interval,
            next_health: now,
            config,
            symbols: HashMap::new(),
            conns: HashMap::new(),
            output,
            stats: EngineStats::default(),
            last_stats: EngineStats::default(),
        }
    }

    pub fn stats(&self) -> &EngineStats {
        &self.stats
    }

    pub async fn run(mut self, mut messages: mpsc::UnboundedReceiver<EngineMsg>) -> EngineStats {
        let mut ticker = tokio::time::interval(Duration::from_millis(250));
        ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                message = messages.recv() => match message {
                    Some(message) => self.handle(message, Instant::now()),
                    // Every connection and the supervisor are gone: we're done.
                    None => break,
                },
                _ = ticker.tick() => self.tick(Instant::now(), Utc::now(), messages.len()),
            }
        }
        info!(stats = ?self.stats, "engine stopped");
        self.stats
    }

    pub fn handle(&mut self, message: EngineMsg, now: Instant) {
        match message {
            EngineMsg::Feed {
                conn,
                received_at,
                events,
            } => {
                for event in events {
                    self.stats.events += 1;
                    self.handle_event(conn, received_at, event, now);
                }
            }
            EngineMsg::ConnUp(conn) => {
                self.conn(conn).up = true;
                // A fresh connection resubscribes everything it carries.
                for state in self.symbols.values_mut() {
                    for replica in state.replicas.iter_mut().filter(|r| r.conn == conn) {
                        replica.synced = false;
                        replica.last_subscribe = now;
                        replica.resubscribe_pending = false;
                        replica.crossed_since = None;
                    }
                }
            }
            EngineMsg::ConnDown(conn) => {
                let state = self.conn(conn);
                state.up = false;
                state.latency_ms = None;
                let symbols: Vec<String> = self
                    .symbols
                    .iter_mut()
                    .filter_map(|(symbol, state)| {
                        let idx = state.replicas.iter().position(|r| r.conn == conn)?;
                        state.replicas[idx].synced = false;
                        (state.source == Some(idx)).then(|| symbol.clone())
                    })
                    .collect();
                for symbol in symbols {
                    self.select_source(&symbol, now, Utc::now());
                }
            }
            EngineMsg::Register { conn, commands } => {
                self.conn(conn).commands = Some(commands);
            }
            EngineMsg::AddSymbols { symbols, conns } => {
                for symbol in symbols {
                    self.symbols.entry(symbol).or_insert_with(|| SymbolState {
                        replicas: conns
                            .iter()
                            .map(|&conn| Replica {
                                conn,
                                book: OrderBook::default(),
                                synced: false,
                                last_msg: None,
                                last_delta: None,
                                synced_at: None,
                                crossed_since: None,
                                last_subscribe: now,
                                resubscribe_pending: false,
                            })
                            .collect(),
                        source: None,
                        last_switch: None,
                        last_activity: None,
                        mean_gap: None,
                        last_source_delta: None,
                        last_quiet_resubscribe: None,
                        added_at: now,
                    });
                }
            }
            EngineMsg::RemoveSymbols(symbols) => {
                for symbol in symbols {
                    self.symbols.remove(&symbol);
                }
            }
        }
    }

    fn conn(&mut self, conn: ConnId) -> &mut ConnState {
        self.conns.entry(conn).or_insert(ConnState {
            up: false,
            latency_ms: None,
            commands: None,
        })
    }

    fn handle_event(
        &mut self,
        conn: ConnId,
        received_at: DateTime<Utc>,
        event: Event,
        now: Instant,
    ) {
        let symbol = match &event {
            Event::Snapshot { symbol, .. }
            | Event::Delta { symbol, .. }
            | Event::TopOfBook { symbol, .. }
            | Event::Desync { symbol, .. } => symbol,
        };
        let Some(state) = self.symbols.get_mut(symbol) else {
            self.stats.ignored += 1;
            return;
        };
        let Some(idx) = state.replicas.iter().position(|r| r.conn == conn) else {
            self.stats.ignored += 1;
            return;
        };
        let is_source = state.source == Some(idx);

        match event {
            Event::Snapshot {
                symbol,
                bids,
                asks,
                exchange_ts,
            } => {
                let replica = &mut state.replicas[idx];
                let mut book = OrderBook::from_levels(&bids, &asks);
                book.last_exchange_ts = exchange_ts.or(Some(received_at));
                let was_synced = replica.synced;
                let old = std::mem::replace(&mut replica.book, book);
                replica.synced = true;
                replica.synced_at = Some(now);
                replica.last_msg = Some(now);
                replica.crossed_since = replica.book.is_crossed().then_some(now);

                if is_source && was_synced {
                    // Express the refresh as level changes so the output stays a
                    // continuous delta stream.
                    let ts = replica.book.last_exchange_ts.unwrap_or(received_at);
                    for (side, price, quantity) in old.diff(&replica.book) {
                        self.stats.updates_written += 1;
                        let _ = self.output.send(Output::Update(PriceLevelUpdateDomain {
                            backtest_timestamp: received_at,
                            exchange_timestamp: ts,
                            price_level: price,
                            quantity,
                            side: book_side(side),
                            symbol: symbol.clone(),
                        }));
                    }
                } else if !is_source {
                    let source_usable = state
                        .source
                        .is_some_and(|s| Self::eligible(&self.conns, &state.replicas[s]));
                    if !source_usable {
                        self.select_source(&symbol, now, received_at);
                    }
                } else {
                    // The source was resyncing: its book was replaced wholesale.
                    self.write_snapshot(&symbol, received_at);
                }
            }
            Event::Delta {
                symbol,
                side,
                price,
                change,
                exchange_ts,
            } => {
                let replica = &mut state.replicas[idx];
                if !replica.synced {
                    return;
                }
                let quantity = match replica.book.apply(side, price, change) {
                    Ok(quantity) => quantity,
                    Err(_) => {
                        self.desync(&symbol, idx, "negative quantity", now);
                        return;
                    }
                };
                replica.last_msg = Some(now);
                replica.last_delta = Some(now);
                if exchange_ts.is_some() {
                    replica.book.last_exchange_ts = exchange_ts;
                }
                match (replica.book.is_crossed(), replica.crossed_since) {
                    (true, None) => replica.crossed_since = Some(now),
                    (false, Some(_)) => replica.crossed_since = None,
                    _ => {}
                }
                if let Some(ts) = exchange_ts {
                    let sample = (received_at - ts).num_microseconds().unwrap_or(0) as f64 / 1000.0;
                    let conn_state = self.conns.entry(conn).or_insert(ConnState {
                        up: true,
                        latency_ms: None,
                        commands: None,
                    });
                    conn_state.latency_ms = Some(match conn_state.latency_ms {
                        Some(mean) => mean + 0.05 * (sample - mean),
                        None => sample,
                    });
                }
                state.last_activity = Some(now);
                if is_source {
                    // One sample per frame: deltas in the same frame share `now`.
                    if let Some(last) = state.last_source_delta
                        && now > last
                    {
                        let gap = now.saturating_duration_since(last).as_secs_f64();
                        state.mean_gap = Some(match state.mean_gap {
                            Some(mean) => mean + 0.1 * (gap - mean),
                            None => gap,
                        });
                    }
                    state.last_source_delta = Some(now);
                    self.stats.updates_written += 1;
                    let _ = self.output.send(Output::Update(PriceLevelUpdateDomain {
                        backtest_timestamp: received_at,
                        exchange_timestamp: exchange_ts.unwrap_or(received_at),
                        price_level: price,
                        quantity,
                        side: book_side(side),
                        symbol,
                    }));
                } else if state.source.is_none() {
                    // A replica may have just become usable (e.g. its book
                    // uncrossed) while the symbol had no source.
                    self.select_source(&symbol, now, received_at);
                }
            }
            Event::TopOfBook {
                symbol,
                best_bid,
                best_ask,
            } => {
                let replica = &state.replicas[idx];
                if replica.synced
                    && (replica.book.best_bid(), replica.book.best_ask()) != (best_bid, best_ask)
                {
                    debug!(
                        %symbol, conn,
                        ours = ?(replica.book.best_bid(), replica.book.best_ask()),
                        theirs = ?(best_bid, best_ask),
                        "top of book mismatch"
                    );
                    self.desync(&symbol, idx, "top of book mismatch", now);
                }
            }
            Event::Desync { symbol, reason } => self.desync(&symbol, idx, &reason, now),
        }
    }

    /// Marks a replica's book as untrustworthy and resubscribes it.
    fn desync(&mut self, symbol: &str, idx: usize, reason: &str, now: Instant) {
        self.stats.desyncs += 1;
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let replica = &mut state.replicas[idx];
        debug!(symbol, conn = replica.conn, reason, "replica desynced");
        replica.synced = false;
        replica.crossed_since = None;
        replica.resubscribe_pending = true;
        let was_source = state.source == Some(idx);
        self.resubscribe(symbol, idx, now);
        if was_source {
            self.select_source(symbol, now, Utc::now());
        }
    }

    fn resubscribe(&mut self, symbol: &str, idx: usize, now: Instant) {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let replica = &mut state.replicas[idx];
        if now.saturating_duration_since(replica.last_subscribe) < self.config.resubscribe_backoff {
            return;
        }
        let Some(conn) = self.conns.get(&replica.conn) else {
            return;
        };
        if !conn.up {
            return;
        }
        if let Some(commands) = &conn.commands {
            replica.last_subscribe = now;
            replica.resubscribe_pending = false;
            self.stats.resubscribes += 1;
            let _ = commands.send(ConnCommand::Resubscribe(vec![symbol.to_owned()]));
        }
    }

    fn eligible(conns: &HashMap<ConnId, ConnState>, replica: &Replica) -> bool {
        replica.synced
            && replica.crossed_since.is_none()
            && conns.get(&replica.conn).is_some_and(|c| c.up)
    }

    fn latency(conns: &HashMap<ConnId, ConnState>, replica: &Replica) -> f64 {
        conns
            .get(&replica.conn)
            .and_then(|c| c.latency_ms)
            .unwrap_or(f64::MAX / 4.0)
    }

    /// The best eligible replica: lowest connection latency, then freshest.
    /// A replica clearly behind its siblings for this symbol is skipped, so a
    /// failover never lands on a subscription that has quietly gone stale.
    fn best_replica(
        conns: &HashMap<ConnId, ConnState>,
        state: &SymbolState,
        stale_after: Duration,
    ) -> Option<usize> {
        let freshness = |r: &Replica| r.last_delta.max(r.synced_at);
        let freshest = state
            .replicas
            .iter()
            .filter(|r| Self::eligible(conns, r))
            .filter_map(|r| r.last_delta)
            .max();
        state
            .replicas
            .iter()
            .enumerate()
            .filter(|(_, r)| Self::eligible(conns, r))
            .filter(|(_, r)| match (freshest, freshness(r)) {
                (Some(freshest), Some(mine)) => {
                    freshest.saturating_duration_since(mine) <= stale_after
                }
                _ => true,
            })
            .min_by(|(_, a), (_, b)| {
                Self::latency(conns, a)
                    .total_cmp(&Self::latency(conns, b))
                    .then_with(|| b.last_msg.cmp(&a.last_msg))
            })
            .map(|(idx, _)| idx)
    }

    /// Re-evaluates the source after it became unusable.
    fn select_source(&mut self, symbol: &str, now: Instant, wall: DateTime<Utc>) {
        let Some(state) = self.symbols.get(symbol) else {
            return;
        };
        let best = Self::best_replica(&self.conns, state, self.config.replica_silence / 2);
        if best != state.source || best.is_none() {
            self.switch_source(symbol, best, now, wall);
        }
    }

    fn switch_source(
        &mut self,
        symbol: &str,
        to: Option<usize>,
        now: Instant,
        wall: DateTime<Utc>,
    ) {
        let Some(state) = self.symbols.get_mut(symbol) else {
            return;
        };
        let from = state.source;
        state.source = to;
        if to.is_none() {
            if from.is_some() {
                warn!(
                    symbol,
                    "no healthy replica; symbol uncovered until one resyncs"
                );
            }
            return;
        }
        state.last_switch = Some(now);
        if from.is_some() {
            self.stats.source_switches += 1;
            debug!(symbol, ?from, ?to, "switched source");
        }
        self.write_snapshot(symbol, wall);
    }

    fn write_snapshot(&mut self, symbol: &str, wall: DateTime<Utc>) {
        let Some(state) = self.symbols.get(symbol) else {
            return;
        };
        let Some(source) = state.source else { return };
        self.stats.snapshots_written += 1;
        let _ = self.output.send(Output::Snapshot(
            state.replicas[source].book.to_snapshot(symbol, wall),
        ));
    }

    /// Periodic work: health checks, periodic snapshots, file rotation, stats.
    pub fn tick(&mut self, now: Instant, wall: DateTime<Utc>, queue_len: usize) {
        if now >= self.next_health {
            self.next_health = now + Duration::from_secs(1);
            self.check_health(now, wall);
        }

        if wall >= self.next_snapshot {
            self.next_snapshot = next_boundary(wall, self.config.snapshot_interval);
            if let (Some(rotation), Some(interval)) =
                (self.next_rotation, self.config.rotate_interval)
                && wall >= rotation
            {
                self.next_rotation = Some(next_boundary(wall, interval));
                let _ = self.output.send(Output::Rotate);
            }
            let symbols: Vec<String> = self.symbols.keys().cloned().collect();
            for symbol in &symbols {
                self.write_snapshot(symbol, wall);
            }
        }

        if now >= self.next_stats {
            self.next_stats = now + self.config.stats_interval;
            self.log_stats(queue_len);
        }
    }

    fn check_health(&mut self, now: Instant, wall: DateTime<Utc>) {
        let config = self.config.clone();
        let mut desyncs = Vec::new();
        let mut resubscribes = Vec::new();
        let mut reselect = Vec::new();
        let mut uncovered = Vec::new();

        for (symbol, state) in &mut self.symbols {
            let source_ok = state
                .source
                .is_some_and(|s| Self::eligible(&self.conns, &state.replicas[s]));
            if !source_ok
                && state
                    .replicas
                    .iter()
                    .any(|r| Self::eligible(&self.conns, r))
            {
                uncovered.push(symbol.clone());
            }
            // Compare level changes only: a sibling's fresh snapshot after a
            // resubscribe must not make this replica look silent.
            let freshest = state.replicas.iter().filter_map(|r| r.last_delta).max();
            for (idx, replica) in state.replicas.iter().enumerate() {
                let conn_up = self.conns.get(&replica.conn).is_some_and(|c| c.up);
                if !conn_up {
                    continue;
                }
                if replica
                    .crossed_since
                    .is_some_and(|t| now.saturating_duration_since(t) > config.crossed_timeout)
                {
                    desyncs.push((symbol.clone(), idx, "crossed book"));
                } else if !replica.synced {
                    let since = now.saturating_duration_since(replica.last_subscribe);
                    // A resubscribe held back by the backoff goes out as soon
                    // as it may; one that never produced a snapshot is retried.
                    if (replica.resubscribe_pending && since >= config.resubscribe_backoff)
                        || since > config.resync_timeout
                    {
                        resubscribes.push((symbol.clone(), idx));
                    }
                } else if let (Some(freshest), Some(last)) =
                    (freshest, replica.last_delta.max(replica.synced_at))
                {
                    // A sibling replica keeps getting data while this one is
                    // silent: this subscription has gone quiet.
                    if freshest.saturating_duration_since(last) > config.replica_silence {
                        desyncs.push((symbol.clone(), idx, "replica silent while siblings active"));
                    }
                }
            }

            // Every replica quiet for much longer than usual: resubscribe one
            // replica at a time (never the source while another exists), so
            // coverage is kept while we check the market really is idle.
            let usual = state.mean_gap.unwrap_or(config.quiet_min.as_secs_f64());
            let threshold = Duration::from_secs_f64((usual * config.quiet_factor).clamp(
                config.quiet_min.as_secs_f64(),
                config.quiet_max.as_secs_f64(),
            ));
            let last_seen = state
                .last_activity
                .into_iter()
                .chain(state.last_quiet_resubscribe)
                .max()
                .unwrap_or(state.added_at);
            if now.saturating_duration_since(last_seen) > threshold {
                let candidate = state
                    .replicas
                    .iter()
                    .enumerate()
                    .filter(|(idx, _)| state.replicas.len() == 1 || Some(*idx) != state.source)
                    .min_by_key(|(_, r)| r.last_subscribe)
                    .map(|(idx, _)| idx);
                if let Some(idx) = candidate {
                    state.last_quiet_resubscribe = Some(now);
                    resubscribes.push((symbol.clone(), idx));
                }
            }

            // Prefer a clearly faster connection, with hysteresis.
            if let Some(source) = state.source
                && !state
                    .last_switch
                    .is_some_and(|t| now.saturating_duration_since(t) < config.min_switch_interval)
                && let Some(best) =
                    Self::best_replica(&self.conns, state, config.replica_silence / 2)
                && best != source
                && Self::latency(&self.conns, &state.replicas[source])
                    - Self::latency(&self.conns, &state.replicas[best])
                    > config.latency_switch_ms
            {
                reselect.push((symbol.clone(), source, best));
            }
        }

        for (symbol, idx, reason) in desyncs {
            self.desync(&symbol, idx, reason, now);
        }
        for (symbol, idx) in resubscribes {
            self.resubscribe(&symbol, idx, now);
        }
        for symbol in uncovered {
            self.select_source(&symbol, now, wall);
        }
        for (symbol, source, best) in reselect {
            // Earlier actions in this pass may have changed things.
            let Some(state) = self.symbols.get(&symbol) else {
                continue;
            };
            if state.source == Some(source) && Self::eligible(&self.conns, &state.replicas[best]) {
                self.switch_source(&symbol, Some(best), now, wall);
            }
        }
    }

    fn log_stats(&mut self, queue_len: usize) {
        let synced = self.symbols.values().filter(|s| s.source.is_some()).count();
        let conns_up = self.conns.values().filter(|c| c.up).count();
        let delta = |now: u64, then: u64| now - then;
        info!(
            symbols = self.symbols.len(),
            covered = synced,
            conns_up,
            conns = self.conns.len(),
            events = delta(self.stats.events, self.last_stats.events),
            updates = delta(self.stats.updates_written, self.last_stats.updates_written),
            snapshots = delta(
                self.stats.snapshots_written,
                self.last_stats.snapshots_written
            ),
            switches = delta(self.stats.source_switches, self.last_stats.source_switches),
            desyncs = delta(self.stats.desyncs, self.last_stats.desyncs),
            resubscribes = delta(self.stats.resubscribes, self.last_stats.resubscribes),
            queue_len,
            "engine stats (since last report)"
        );
        self.last_stats = self.stats.clone();
    }
}

#[cfg(test)]
mod tests {
    use crossbeam::channel::{Receiver, unbounded};

    use super::*;
    use crate::book::LevelChange;

    struct Harness {
        engine: Engine,
        output: Receiver<Output>,
        commands: Vec<mpsc::UnboundedReceiver<ConnCommand>>,
        start: Instant,
        wall: DateTime<Utc>,
    }

    fn harness() -> Harness {
        let (tx, output) = unbounded();
        let start = Instant::now();
        let wall = DateTime::from_timestamp(1_700_000_000, 0).unwrap();
        let config = EngineConfig {
            quiet_min: Duration::from_secs(3600),
            ..EngineConfig::default()
        };
        let mut engine = Engine::new(config, tx, start, wall);
        let mut commands = Vec::new();
        for conn in 0..2 {
            let (tx, rx) = mpsc::unbounded_channel();
            engine.handle(EngineMsg::Register { conn, commands: tx }, start);
            engine.handle(EngineMsg::ConnUp(conn), start);
            commands.push(rx);
        }
        engine.handle(
            EngineMsg::AddSymbols {
                symbols: vec!["A".into()],
                conns: vec![0, 1],
            },
            start,
        );
        Harness {
            engine,
            output,
            commands,
            start,
            wall,
        }
    }

    impl Harness {
        fn feed(&mut self, conn: ConnId, after: Duration, event: Event) {
            self.engine.handle(
                EngineMsg::Feed {
                    conn,
                    received_at: self.wall,
                    events: vec![event],
                },
                self.start + after,
            );
        }

        fn outputs(&self) -> Vec<Output> {
            self.output.try_iter().collect()
        }
    }

    fn snapshot(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Event {
        Event::Snapshot {
            symbol: "A".into(),
            bids: bids.to_vec(),
            asks: asks.to_vec(),
            exchange_ts: None,
        }
    }

    fn delta(side: Side, price: i64, qty: i64) -> Event {
        Event::Delta {
            symbol: "A".into(),
            side,
            price,
            change: LevelChange::Set(qty),
            exchange_ts: None,
        }
    }

    type Levels = Vec<(i64, i64)>;

    fn snapshot_levels(output: &Output) -> Option<(Levels, Levels)> {
        match output {
            Output::Snapshot(s) => Some((
                s.bids.iter().map(|l| (l.price_level, l.quantity)).collect(),
                s.asks.iter().map(|l| (l.price_level, l.quantity)).collect(),
            )),
            _ => None,
        }
    }

    #[test]
    fn writes_only_the_source_replica() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        h.feed(1, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        let out = h.outputs();
        assert_eq!(out.len(), 1, "one snapshot when the first replica syncs");
        assert!(snapshot_levels(&out[0]).is_some());

        h.feed(0, Duration::from_millis(1), delta(Side::Bid, 450, 2));
        h.feed(1, Duration::from_millis(2), delta(Side::Bid, 450, 2));
        let out = h.outputs();
        assert_eq!(
            out.len(),
            1,
            "duplicate from the other replica is not written"
        );
        let Output::Update(update) = &out[0] else {
            panic!("expected update")
        };
        assert_eq!(
            (update.price_level, update.quantity, update.side),
            (450, 2, BookSide::Bid)
        );
    }

    #[test]
    fn fails_over_with_snapshot_on_disconnect() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        h.feed(1, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        // Replica 1 saw an update that replica 0 missed.
        h.feed(1, Duration::from_millis(1), delta(Side::Ask, 550, 7));
        h.outputs();

        h.engine
            .handle(EngineMsg::ConnDown(0), h.start + Duration::from_millis(2));
        let out = h.outputs();
        assert_eq!(out.len(), 1);
        assert_eq!(
            snapshot_levels(&out[0]),
            Some((vec![(400, 1)], vec![(550, 7), (600, 1)])),
            "snapshot comes from the new source's book"
        );
        assert_eq!(h.engine.stats().source_switches, 1);

        h.feed(1, Duration::from_millis(3), delta(Side::Bid, 410, 1));
        assert!(matches!(h.outputs()[..], [Output::Update(_)]));
    }

    #[test]
    fn desync_switches_source_and_resubscribes() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        h.feed(1, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        h.outputs();

        let later = Duration::from_secs(11);
        h.feed(
            0,
            later,
            Event::Desync {
                symbol: "A".into(),
                reason: "sequence gap".into(),
            },
        );
        assert!(snapshot_levels(&h.outputs()[0]).is_some());
        assert_eq!(
            h.commands[0].try_recv().ok(),
            Some(ConnCommand::Resubscribe(vec!["A".into()]))
        );

        // The resubscribed replica's snapshot doesn't steal the source back.
        h.feed(0, later, snapshot(&[(400, 1)], &[]));
        assert!(h.outputs().is_empty());
    }

    #[test]
    fn top_of_book_mismatch_desyncs() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        h.feed(1, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        h.outputs();
        h.feed(
            0,
            Duration::from_secs(11),
            Event::TopOfBook {
                symbol: "A".into(),
                best_bid: Some(400),
                best_ask: Some(550),
            },
        );
        assert_eq!(h.engine.stats().desyncs, 1);
        assert!(h.commands[0].try_recv().is_ok());
    }

    #[test]
    fn silent_replica_is_resubscribed() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        h.feed(1, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        // Only replica 1 keeps receiving updates.
        for second in 1..=90 {
            h.feed(
                1,
                Duration::from_secs(second),
                delta(Side::Bid, 400, second as i64),
            );
        }
        h.outputs();
        h.engine.tick(h.start + Duration::from_secs(90), h.wall, 0);
        assert_eq!(
            h.commands[0].try_recv().ok(),
            Some(ConnCommand::Resubscribe(vec!["A".into()]))
        );
        assert!(h.commands[1].try_recv().is_err());
        assert_eq!(h.engine.stats().source_switches, 1);
    }

    #[test]
    fn negative_quantity_desyncs() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        h.outputs();
        h.engine.handle(
            EngineMsg::Feed {
                conn: 0,
                received_at: h.wall,
                events: vec![Event::Delta {
                    symbol: "A".into(),
                    side: Side::Bid,
                    price: 400,
                    change: LevelChange::Add(-5),
                    exchange_ts: None,
                }],
            },
            h.start + Duration::from_secs(11),
        );
        assert_eq!(h.engine.stats().desyncs, 1);
        assert!(h.outputs().iter().all(|o| !matches!(o, Output::Update(_))));
    }

    #[test]
    fn periodic_snapshots_on_wall_clock_boundaries() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        h.outputs();
        h.engine
            .tick(h.start, h.wall + chrono::Duration::seconds(30), 0);
        assert!(h.outputs().is_empty());
        h.engine
            .tick(h.start, h.wall + chrono::Duration::seconds(61), 0);
        assert_eq!(h.outputs().len(), 1);
    }

    #[test]
    fn switches_to_a_clearly_faster_replica() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        h.feed(1, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        for i in 1..=20 {
            for (conn, lag_ms) in [(0, 200), (1, 10)] {
                h.feed(
                    conn,
                    Duration::from_secs(i),
                    Event::Delta {
                        symbol: "A".into(),
                        side: Side::Bid,
                        price: 400,
                        change: LevelChange::Set(i as i64),
                        exchange_ts: Some(h.wall - chrono::Duration::milliseconds(lag_ms)),
                    },
                );
            }
        }
        h.outputs();
        // Too soon after the initial selection: no switch yet.
        h.engine.tick(h.start + Duration::from_secs(20), h.wall, 0);
        assert_eq!(h.engine.stats().source_switches, 0);
        h.engine.tick(h.start + Duration::from_secs(31), h.wall, 0);
        assert_eq!(h.engine.stats().source_switches, 1);
        assert!(snapshot_levels(&h.outputs()[0]).is_some());
    }

    #[test]
    fn covers_symbol_once_a_replica_uncrosses() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        // Replica 1's book is momentarily crossed.
        h.feed(1, Duration::ZERO, snapshot(&[(650, 1)], &[(600, 1)]));
        h.outputs();
        h.engine
            .handle(EngineMsg::ConnDown(0), h.start + Duration::from_millis(1));
        assert!(h.outputs().is_empty(), "no healthy replica yet");

        h.feed(1, Duration::from_millis(2), delta(Side::Bid, 650, 0));
        let out = h.outputs();
        assert_eq!(
            snapshot_levels(&out[0]),
            Some((vec![], vec![(600, 1)])),
            "the uncrossed replica becomes the source straight away"
        );
    }

    #[test]
    fn backed_off_resubscribe_is_retried_promptly() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[]));
        // A gap 2s after connecting: inside the 10s resubscribe backoff.
        h.feed(
            0,
            Duration::from_secs(2),
            Event::Desync {
                symbol: "A".into(),
                reason: "sequence gap".into(),
            },
        );
        assert!(h.commands[0].try_recv().is_err());
        h.engine.tick(h.start + Duration::from_secs(11), h.wall, 0);
        assert_eq!(
            h.commands[0].try_recv().ok(),
            Some(ConnCommand::Resubscribe(vec!["A".into()]))
        );
    }

    #[test]
    fn refreshed_source_snapshot_is_written_as_deltas() {
        let mut h = harness();
        h.feed(0, Duration::ZERO, snapshot(&[(400, 1)], &[(600, 1)]));
        h.outputs();
        h.feed(
            0,
            Duration::from_secs(1),
            snapshot(&[(400, 3)], &[(600, 1)]),
        );
        let out = h.outputs();
        assert_eq!(out.len(), 1);
        let Output::Update(update) = &out[0] else {
            panic!("expected update")
        };
        assert_eq!((update.price_level, update.quantity), (400, 3));
    }
}
