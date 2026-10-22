//! Symbol discovery and connection management.
//!
//! The supervisor periodically asks the exchange for its live symbols, diffs
//! that against what we carry, and assigns new symbols to shards. A shard is a
//! group of up to `symbols_per_connection` symbols carried by `replicas`
//! identical, independent connections (Extension 1).

use std::collections::{BTreeSet, HashMap, HashSet};
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;
use std::time::Duration;

use tokio::sync::{mpsc, watch};
use tokio::task::JoinSet;
use tracing::{info, warn};

use crate::connection::{ConnConfig, ConnectLimiter, Connection, sleep_or_shutdown};
use crate::engine::{ConnCommand, ConnId, EngineMsg};
use crate::exchange::{Discovered, Exchange};

#[derive(Clone, Debug)]
pub struct SupervisorConfig {
    pub replicas: usize,
    pub symbols_per_connection: usize,
    pub discovery_interval: Duration,
    /// Consecutive complete discoveries a symbol must be missing from before
    /// it is unsubscribed.
    pub removal_confirmations: u32,
    pub connects_per_second: f64,
    /// Only carry the first N symbols (for local testing).
    pub max_symbols: Option<usize>,
    pub connection: ConnConfig,
}

impl Default for SupervisorConfig {
    fn default() -> Self {
        Self {
            replicas: 2,
            symbols_per_connection: 200,
            discovery_interval: Duration::from_secs(60),
            removal_confirmations: 2,
            connects_per_second: 5.0,
            max_symbols: None,
            connection: ConnConfig::default(),
        }
    }
}

struct Shard {
    symbols: HashSet<String>,
    conns: Vec<(ConnId, mpsc::UnboundedSender<ConnCommand>)>,
}

pub struct Supervisor<E: Exchange> {
    exchange: Arc<E>,
    config: SupervisorConfig,
    engine: mpsc::UnboundedSender<EngineMsg>,
    shutdown: watch::Receiver<bool>,
    limiter: Arc<ConnectLimiter>,
    shards: Vec<Shard>,
    location: HashMap<String, usize>,
    tasks: JoinSet<()>,
    next_conn: ConnId,
    /// Consecutive complete discoveries each symbol has been missing from.
    missing: HashMap<String, u32>,
}

impl<E: Exchange> Supervisor<E> {
    pub fn new(
        exchange: Arc<E>,
        config: SupervisorConfig,
        engine: mpsc::UnboundedSender<EngineMsg>,
        shutdown: watch::Receiver<bool>,
    ) -> Self {
        Self {
            limiter: Arc::new(ConnectLimiter::new(config.connects_per_second)),
            exchange,
            config,
            engine,
            shutdown,
            shards: Vec::new(),
            location: HashMap::new(),
            tasks: JoinSet::new(),
            next_conn: 0,
            missing: HashMap::new(),
        }
    }

    pub async fn run(mut self) {
        loop {
            let discovered = tokio::select! {
                result = self.exchange.discover() => result,
                _ = self.shutdown.changed() => break,
            };
            match discovered {
                Ok(mut discovered) => {
                    if let Some(max) = self.config.max_symbols {
                        discovered.symbols.truncate(max);
                    }
                    self.reconcile(discovered);
                }
                // Never drop subscriptions because discovery failed.
                Err(error) => warn!(error = %format!("{error:#}"), "symbol discovery failed"),
            }
            if sleep_or_shutdown(self.config.discovery_interval, &mut self.shutdown).await {
                break;
            }
        }

        info!("stopping connections");
        // Dropping the command senders also stops any connection that missed
        // the shutdown signal.
        self.shards.clear();
        let tasks = std::mem::take(&mut self.tasks);
        if tokio::time::timeout(Duration::from_secs(10), tasks.join_all())
            .await
            .is_err()
        {
            warn!("connections did not stop in time");
        }
    }

    fn reconcile(&mut self, discovered: Discovered) {
        let live: HashSet<String> = discovered.symbols.into_iter().collect();
        self.missing.retain(|symbol, _| !live.contains(symbol));

        // Paginated listings shift while we page through them, so a symbol
        // must be missing from consecutive complete listings before we drop
        // it. An incomplete listing never removes anything.
        let mut removed = HashSet::new();
        if discovered.complete {
            for symbol in self.location.keys().filter(|s| !live.contains(*s)) {
                let misses = self.missing.entry(symbol.clone()).or_default();
                *misses += 1;
                if *misses >= self.config.removal_confirmations.max(1) {
                    removed.insert(symbol.clone());
                }
            }
            for symbol in &removed {
                self.missing.remove(symbol);
            }
        }

        let added: Vec<String> = {
            let mut added: Vec<_> = live
                .into_iter()
                .filter(|s| !self.location.contains_key(s))
                .collect();
            added.sort_unstable();
            added
        };

        self.remove(&removed);
        self.add(&added);
        info!(
            added = added.len(),
            removed = removed.len(),
            symbols = self.location.len(),
            shards = self.shards.len(),
            connections = self.shards.len() * self.config.replicas,
            "symbols reconciled"
        );
    }

    fn remove(&mut self, removed: &HashSet<String>) {
        let mut by_shard: HashMap<usize, Vec<String>> = HashMap::new();
        for symbol in removed {
            if let Some(shard) = self.location.remove(symbol) {
                by_shard.entry(shard).or_default().push(symbol.clone());
            }
        }
        if removed.is_empty() {
            return;
        }
        let _ = self
            .engine
            .send(EngineMsg::RemoveSymbols(removed.iter().cloned().collect()));
        for (shard, symbols) in by_shard {
            let shard = &mut self.shards[shard];
            for symbol in &symbols {
                shard.symbols.remove(symbol);
            }
            for (_, commands) in &shard.conns {
                let _ = commands.send(ConnCommand::Unsubscribe(symbols.clone()));
            }
        }
    }

    fn add(&mut self, added: &[String]) {
        let capacity = self.config.symbols_per_connection.max(1);
        let mut remaining = added;

        // Fill holes in existing shards first.
        for idx in 0..self.shards.len() {
            if remaining.is_empty() {
                return;
            }
            let free = capacity.saturating_sub(self.shards[idx].symbols.len());
            if free == 0 {
                continue;
            }
            let (batch, rest) = remaining.split_at(free.min(remaining.len()));
            remaining = rest;
            let shard = &mut self.shards[idx];
            for symbol in batch {
                shard.symbols.insert(symbol.clone());
                self.location.insert(symbol.clone(), idx);
            }
            // The engine must know the symbols before any data can arrive.
            let _ = self.engine.send(EngineMsg::AddSymbols {
                symbols: batch.to_vec(),
                conns: shard.conns.iter().map(|(id, _)| *id).collect(),
            });
            for (_, commands) in &shard.conns {
                let _ = commands.send(ConnCommand::Subscribe(batch.to_vec()));
            }
        }

        for batch in remaining.chunks(capacity) {
            self.spawn_shard(batch);
        }
    }

    fn spawn_shard(&mut self, symbols: &[String]) {
        let idx = self.shards.len();
        let replicas = self.config.replicas.max(1);
        let conn_ids: Vec<ConnId> = (0..replicas).map(|r| self.next_conn + r).collect();
        self.next_conn += replicas;

        let _ = self.engine.send(EngineMsg::AddSymbols {
            symbols: symbols.to_vec(),
            conns: conn_ids.clone(),
        });

        let shard_up = Arc::new(AtomicUsize::new(0));
        let shard_offset = Duration::from_secs_f64(
            rand::Rng::gen_range(&mut rand::thread_rng(), 0.0..1.0) / replicas as f64,
        );
        let mut conns = Vec::new();
        for (replica, &id) in conn_ids.iter().enumerate() {
            let (commands_tx, commands_rx) = mpsc::unbounded_channel();
            let _ = self.engine.send(EngineMsg::Register {
                conn: id,
                commands: commands_tx.clone(),
            });
            // Fixed, evenly spaced recycle phases per replica, plus a random
            // per-shard offset so shards don't all recycle at the same moment.
            let next_recycle = self.config.connection.recycle_interval.map(|interval| {
                tokio::time::Instant::now()
                    + shard_offset.mul_f64(interval.as_secs_f64())
                    + interval.mul_f64((replica + 1) as f64 / replicas as f64)
            });
            let connection = Connection {
                id,
                exchange: self.exchange.clone(),
                symbols: symbols.iter().cloned().collect::<BTreeSet<_>>(),
                commands: commands_rx,
                engine: self.engine.clone(),
                limiter: self.limiter.clone(),
                config: self.config.connection.clone(),
                shutdown: self.shutdown.clone(),
                next_recycle,
                shard_up: shard_up.clone(),
                replicas,
            };
            self.tasks.spawn(connection.run());
            conns.push((id, commands_tx));
        }

        for symbol in symbols {
            self.location.insert(symbol.clone(), idx);
        }
        self.shards.push(Shard {
            symbols: symbols.iter().cloned().collect(),
            conns,
        });
    }
}
