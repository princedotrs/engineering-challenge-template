use std::io::IsTerminal;
use std::path::PathBuf;
use std::time::Duration;

use anyhow::bail;
use clap::{Args, Parser, Subcommand};
use engineering_challenge::collector::{self, CollectorConfig};
use engineering_challenge::connection::ConnConfig;
use engineering_challenge::dump;
use engineering_challenge::engine::EngineConfig;
use engineering_challenge::exchange::Exchange;
use engineering_challenge::exchange::kalshi::{self, Kalshi, KalshiAuth};
use engineering_challenge::exchange::polymarket::{self, Polymarket};
use engineering_challenge::output::Target;
use engineering_challenge::probe::{self, ProbeConfig};
use engineering_challenge::supervisor::SupervisorConfig;
use tracing::{info, warn};

/// Order book collector: streams every live symbol's book deltas, plus a full
/// snapshot every minute, as aligned SBE.
#[derive(Parser)]
#[command(version)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// Collect Polymarket order books (no API key needed).
    Polymarket {
        #[command(flatten)]
        common: Common,
        #[arg(long, default_value = polymarket::DEFAULT_REST_URL)]
        rest_url: String,
        #[arg(long, default_value = polymarket::DEFAULT_WS_URL)]
        ws_url: String,
        /// Don't verify our books against the best bid/ask the exchange sends
        /// with every price change.
        #[arg(long)]
        no_verify_top_of_book: bool,
    },
    /// Collect Kalshi order books (needs an API key).
    Kalshi {
        #[command(flatten)]
        common: Common,
        #[command(flatten)]
        credentials: KalshiCredentials,
    },
    /// Latency experiments: compare parallel connections to the same symbols.
    Probe {
        #[arg(value_enum)]
        exchange: ExchangeName,
        /// Parallel connections to compare.
        #[arg(long, default_value_t = 4)]
        connections: usize,
        /// Symbols every connection subscribes to.
        #[arg(long, default_value_t = 50)]
        symbols: usize,
        #[arg(long, default_value_t = 120)]
        duration_secs: u64,
        /// Bind connection i to local TCP port `source_port_base + i`.
        #[arg(long)]
        source_port_base: Option<u16>,
        #[command(flatten)]
        kalshi: KalshiCredentials,
    },
    /// Print an output file as text and sanity-check it by replaying the books.
    Dump {
        file: PathBuf,
        /// Only print the summary.
        #[arg(long)]
        quiet: bool,
    },
}

#[derive(Clone, Copy, clap::ValueEnum)]
enum ExchangeName {
    Polymarket,
    Kalshi,
}

#[derive(Args)]
struct KalshiCredentials {
    /// Kalshi API key id.
    #[arg(long, env = "KALSHI_API_KEY_ID")]
    key_id: Option<String>,
    /// Path to the RSA private key (PEM) that belongs to the API key.
    #[arg(long, env = "KALSHI_PRIVATE_KEY_PATH")]
    private_key: Option<PathBuf>,
    /// Use Kalshi's demo environment instead of production.
    #[arg(long)]
    demo: bool,
    #[arg(long)]
    kalshi_rest_url: Option<String>,
    #[arg(long)]
    kalshi_ws_url: Option<String>,
}

impl KalshiCredentials {
    fn exchange(self) -> anyhow::Result<Kalshi> {
        let (Some(key_id), Some(private_key)) = (self.key_id, self.private_key) else {
            bail!(
                "Kalshi's order book feed needs an API key: pass --key-id and --private-key \
                 (or set KALSHI_API_KEY_ID and KALSHI_PRIVATE_KEY_PATH)"
            );
        };
        let (default_rest, default_ws) = if self.demo {
            (kalshi::DEMO_REST_URL, kalshi::DEMO_WS_URL)
        } else {
            (kalshi::PROD_REST_URL, kalshi::PROD_WS_URL)
        };
        Kalshi::new(
            self.kalshi_rest_url.unwrap_or_else(|| default_rest.into()),
            self.kalshi_ws_url.unwrap_or_else(|| default_ws.into()),
            KalshiAuth::from_pem_file(key_id, &private_key)?,
        )
    }
}

#[derive(Args)]
struct Common {
    /// Output file; `-` writes to stdout.
    #[arg(long, short, default_value = "-", conflicts_with = "output_dir")]
    output: PathBuf,
    /// Write rotating, timestamped files to this directory instead.
    #[arg(long)]
    output_dir: Option<PathBuf>,
    /// How often to start a new file in `--output-dir`.
    #[arg(long, default_value_t = 60)]
    rotate_minutes: u64,
    /// Redundant websockets per group of symbols.
    #[arg(long, default_value_t = 2)]
    replicas: usize,
    /// Symbols per websocket (default: 200 for Polymarket, 250 for Kalshi).
    #[arg(long)]
    symbols_per_connection: Option<usize>,
    /// How often to look for new and closed symbols.
    #[arg(long, default_value_t = 60)]
    discovery_interval_secs: u64,
    /// How often to write a full snapshot of every book.
    #[arg(long, default_value_t = 60)]
    snapshot_interval_secs: u64,
    /// Recycle every websocket this often; 0 disables recycling.
    #[arg(long, default_value_t = 30)]
    recycle_minutes: u64,
    /// Upper bound on new websocket connections per second.
    #[arg(long, default_value_t = 5.0)]
    connects_per_second: f64,
    /// Only collect the first N symbols (handy for trying things out).
    #[arg(long)]
    max_symbols: Option<usize>,
}

impl Common {
    fn config(
        &self,
        exchange: &str,
        default_per_connection: usize,
    ) -> anyhow::Result<CollectorConfig> {
        let target = match &self.output_dir {
            Some(dir) => Target::Directory {
                dir: dir.clone(),
                prefix: exchange.to_owned(),
            },
            None if self.output.as_os_str() == "-" => {
                if std::io::stdout().is_terminal() {
                    bail!(
                        "refusing to write binary output to a terminal; redirect stdout \
                         (`> book.sbe`), or use --output / --output-dir"
                    );
                }
                Target::File(PathBuf::from("/dev/stdout"))
            }
            None => Target::File(self.output.clone()),
        };
        Ok(CollectorConfig {
            engine: EngineConfig {
                snapshot_interval: Duration::from_secs(self.snapshot_interval_secs.max(1)),
                rotate_interval: self
                    .output_dir
                    .as_ref()
                    .map(|_| Duration::from_secs(self.rotate_minutes.max(1) * 60)),
                ..EngineConfig::default()
            },
            supervisor: SupervisorConfig {
                replicas: self.replicas.max(1),
                symbols_per_connection: self
                    .symbols_per_connection
                    .unwrap_or(default_per_connection),
                discovery_interval: Duration::from_secs(self.discovery_interval_secs.max(1)),
                removal_confirmations: 2,
                connects_per_second: self.connects_per_second,
                max_symbols: self.max_symbols,
                connection: ConnConfig {
                    recycle_interval: (self.recycle_minutes > 0)
                        .then(|| Duration::from_secs(self.recycle_minutes * 60)),
                    ..ConnConfig::default()
                },
            },
            target,
        })
    }
}

/// Resolves on the first SIGINT (or SIGTERM). A second SIGINT exits at once.
async fn shutdown_signal() {
    #[cfg(unix)]
    {
        use tokio::signal::unix::{SignalKind, signal};
        let mut term = signal(SignalKind::terminate()).expect("SIGTERM handler");
        tokio::select! {
            _ = tokio::signal::ctrl_c() => {}
            _ = term.recv() => {}
        }
    }
    #[cfg(not(unix))]
    let _ = tokio::signal::ctrl_c().await;

    info!("signal received; shutting down (press Ctrl-C again to force)");
    tokio::spawn(async {
        let _ = tokio::signal::ctrl_c().await;
        warn!("forced exit");
        std::process::exit(130);
    });
}

async fn collect<E: Exchange>(exchange: E, config: CollectorConfig) -> anyhow::Result<()> {
    let summary = collector::run(exchange, config, shutdown_signal()).await?;
    info!(?summary, "done");
    Ok(())
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    // stdout carries the data, so logs go to stderr.
    tracing_subscriber::fmt()
        .with_writer(std::io::stderr)
        .with_env_filter(
            tracing_subscriber::EnvFilter::try_from_default_env().unwrap_or_else(|_| "info".into()),
        )
        .init();
    engineering_challenge::exchange::install_crypto_provider();

    match Cli::parse().command {
        Command::Polymarket {
            common,
            rest_url,
            ws_url,
            no_verify_top_of_book,
        } => {
            let config = common.config("polymarket", 200)?;
            collect(
                Polymarket::new(rest_url, ws_url, !no_verify_top_of_book)?,
                config,
            )
            .await
        }
        Command::Kalshi {
            common,
            credentials,
        } => {
            let config = common.config("kalshi", 250)?;
            collect(credentials.exchange()?, config).await
        }
        Command::Probe {
            exchange,
            connections,
            symbols,
            duration_secs,
            source_port_base,
            kalshi,
        } => {
            let config = ProbeConfig {
                connections: connections.max(1),
                symbols: symbols.max(1),
                duration: Duration::from_secs(duration_secs),
                source_port_base,
            };
            match exchange {
                ExchangeName::Polymarket => {
                    let exchange = Polymarket::new(
                        polymarket::DEFAULT_REST_URL.into(),
                        polymarket::DEFAULT_WS_URL.into(),
                        false,
                    )?;
                    probe::run(exchange, config).await
                }
                ExchangeName::Kalshi => probe::run(kalshi.exchange()?, config).await,
            }
        }
        Command::Dump { file, quiet } => {
            let stdout = std::io::stdout();
            let summary = dump::dump(&file, quiet, &mut stdout.lock())?;
            eprintln!(
                "snapshots={} updates={} symbols={} updates_without_snapshot={} crossed_after_update={}",
                summary.snapshots,
                summary.updates,
                summary.symbols,
                summary.updates_without_snapshot,
                summary.crossed_after_update,
            );
            Ok(())
        }
    }
}
