//! Wires the supervisor, connections, engine and output together, and shuts
//! them down in order.

use std::future::Future;
use std::sync::Arc;

use tokio::sync::{mpsc, watch};
use tokio::time::Instant;
use tracing::{error, info};

use crate::engine::{Engine, EngineConfig, EngineStats};
use crate::exchange::Exchange;
use crate::output::{self, Target, WriterStats};
use crate::supervisor::{Supervisor, SupervisorConfig};

#[derive(Clone, Debug)]
pub struct CollectorConfig {
    pub target: Target,
    pub engine: EngineConfig,
    pub supervisor: SupervisorConfig,
}

#[derive(Debug)]
pub struct Summary {
    pub engine: EngineStats,
    pub writer: WriterStats,
}

/// Runs the collector until `shutdown` resolves, then closes every websocket,
/// writes out everything already received, and closes the output file.
pub async fn run<E: Exchange>(
    exchange: E,
    config: CollectorConfig,
    shutdown: impl Future<Output = ()>,
) -> anyhow::Result<Summary> {
    info!(exchange = exchange.name(), "starting collector");
    let output = output::spawn(config.target.clone())?;

    let (engine_tx, engine_rx) = mpsc::unbounded_channel();
    let engine = Engine::new(
        config.engine,
        output.sender.clone(),
        Instant::now(),
        chrono::Utc::now(),
    );
    let engine_task = tokio::spawn(engine.run(engine_rx));

    let (shutdown_tx, shutdown_rx) = watch::channel(false);
    let supervisor = Supervisor::new(
        Arc::new(exchange),
        config.supervisor,
        engine_tx,
        shutdown_rx,
    );
    let supervisor_task = tokio::spawn(supervisor.run());

    // Stop on request, or if the writer dies (disk full, closed pipe, ...),
    // since collecting without writing would silently lose data.
    let writer_died = async {
        while !output.is_finished() {
            tokio::time::sleep(std::time::Duration::from_millis(500)).await;
        }
    };
    tokio::select! {
        _ = shutdown => info!("shutdown requested: closing websockets"),
        _ = writer_died => error!("output writer stopped unexpectedly: shutting down"),
    }
    let _ = shutdown_tx.send(true);

    // Connections close their sockets and exit; once the last one is gone the
    // engine's queue closes, so it drains every frame we already received.
    let supervisor = supervisor_task.await;
    let engine = engine_task.await;
    info!("engine drained: closing output");
    // Close the output even if a task failed, so the last chunk is written.
    let writer = tokio::task::spawn_blocking(move || output.finish()).await??;
    info!(?writer, "output closed");
    supervisor?;
    Ok(Summary {
        engine: engine?,
        writer,
    })
}
