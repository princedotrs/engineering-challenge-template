//! Output: a dedicated thread that owns the [`AlignedSbeWriter`].
//!
//! Encoding and disk writes happen off the async runtime so a slow disk can
//! never stall websocket reads. The engine sends [`Output`] messages over an
//! unbounded channel; [`Output::Rotate`] closes the current file and opens the
//! next one.

use std::path::{Path, PathBuf};
use std::thread::JoinHandle;

use anyhow::Context;
use chrono::Utc;
use crossbeam::channel::{Receiver, Sender, unbounded};
use tracing::info;

use crate::util::AlignedSbeWriter;
use crate::util::dtos::challenge_orderbook::{PriceLevelUpdateDomain, SnapshotDomain};

/// Chunk size expected by [`crate::util::AlignedSbeReader`].
pub const CHUNK_SIZE: usize = 64 << 20;

pub enum Output {
    Snapshot(SnapshotDomain),
    Update(PriceLevelUpdateDomain),
    /// Close the current file and start a new one.
    Rotate,
}

#[derive(Clone, Debug)]
pub enum Target {
    /// A single file, or `/dev/stdout`.
    File(PathBuf),
    /// Timestamped files in a directory, rotated by the engine.
    Directory { dir: PathBuf, prefix: String },
}

impl Target {
    fn next_path(&self) -> PathBuf {
        match self {
            Target::File(path) => path.clone(),
            Target::Directory { dir, prefix } => dir.join(format!(
                "{prefix}-{}.sbe",
                Utc::now().format("%Y%m%dT%H%M%S%.3fZ")
            )),
        }
    }

    pub fn rotates(&self) -> bool {
        matches!(self, Target::Directory { .. })
    }
}

#[derive(Debug, Default)]
pub struct WriterStats {
    pub snapshots: u64,
    pub updates: u64,
    pub files: u64,
}

pub struct OutputHandle {
    pub sender: Sender<Output>,
    thread: JoinHandle<anyhow::Result<WriterStats>>,
}

impl OutputHandle {
    /// True if the writer thread has stopped (it only stops early on error).
    pub fn is_finished(&self) -> bool {
        self.thread.is_finished()
    }

    /// Waits until everything sent so far is written and the file is closed.
    /// Every sender must be dropped first.
    pub fn finish(self) -> anyhow::Result<WriterStats> {
        drop(self.sender);
        self.thread
            .join()
            .map_err(|_| anyhow::anyhow!("output thread panicked"))?
    }
}

pub fn spawn(target: Target) -> anyhow::Result<OutputHandle> {
    if let Target::Directory { dir, .. } = &target {
        std::fs::create_dir_all(dir)
            .with_context(|| format!("creating output directory {}", dir.display()))?;
    }
    // Open the first file up front so a bad path fails at startup.
    let first = open(&target.next_path())?;
    let (sender, receiver) = unbounded();
    let thread = std::thread::Builder::new()
        .name("sbe-writer".into())
        .spawn(move || run(target, first, receiver))?;
    Ok(OutputHandle { sender, thread })
}

fn open(path: &Path) -> anyhow::Result<(AlignedSbeWriter, PathBuf)> {
    let writer = AlignedSbeWriter::create(path, CHUNK_SIZE)
        .with_context(|| format!("opening output {}", path.display()))?;
    info!(path = %path.display(), "writing to");
    Ok((writer, path.to_owned()))
}

fn close(mut writer: AlignedSbeWriter, path: &Path) -> anyhow::Result<()> {
    writer.flush()?;
    drop(writer);
    info!(path = %path.display(), "closed output");
    Ok(())
}

fn run(
    target: Target,
    first: (AlignedSbeWriter, PathBuf),
    receiver: Receiver<Output>,
) -> anyhow::Result<WriterStats> {
    let (mut writer, mut path) = first;
    let mut stats = WriterStats {
        files: 1,
        ..WriterStats::default()
    };
    // The channel closes once the engine has sent its last message.
    for message in receiver {
        match message {
            Output::Snapshot(snapshot) => {
                writer.write_msg(&snapshot)?;
                stats.snapshots += 1;
            }
            Output::Update(update) => {
                writer.write_msg(&update)?;
                stats.updates += 1;
            }
            Output::Rotate if target.rotates() => {
                close(writer, &path)?;
                (writer, path) = open(&target.next_path())?;
                stats.files += 1;
            }
            Output::Rotate => {}
        }
    }
    close(writer, &path)?;
    Ok(stats)
}
