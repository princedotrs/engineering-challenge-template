//! Reads an output file back with [`AlignedSbeReader`] and prints it as text,
//! one message per line. Useful to eyeball and verify what the collector wrote.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use crate::book::{LevelChange, OrderBook, Side};
use crate::util::dtos::challenge_orderbook::{
    AnyMessage, BookSide, PriceLevelUpdateDomain, SnapshotDomain,
};
use crate::util::{AlignedSbeReader, OrderbookSchema};

#[derive(Debug, Default)]
pub struct DumpSummary {
    pub snapshots: u64,
    pub updates: u64,
    pub symbols: usize,
    /// Updates for a symbol before any snapshot of it (should be zero).
    pub updates_without_snapshot: u64,
    /// Times a replayed book crossed (should be rare and transient).
    pub crossed_after_update: u64,
    /// Every book as of the end of the file.
    pub books: HashMap<String, OrderBook>,
}

/// Prints every message (unless `quiet`) and replays the books to sanity-check
/// the stream.
pub fn dump(path: &Path, quiet: bool, out: &mut impl Write) -> anyhow::Result<DumpSummary> {
    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(path)?;
    let mut books: HashMap<String, OrderBook> = HashMap::new();
    let mut summary = DumpSummary::default();

    while let Some(message) = reader.read_msg()? {
        match message {
            AnyMessage::Snapshot(decoder) => {
                let snapshot = SnapshotDomain::try_from_decoder(decoder)?;
                summary.snapshots += 1;
                if !quiet {
                    let levels = |levels: Vec<(i64, i64)>| {
                        levels
                            .iter()
                            .map(|(p, q)| format!("{q}@{p}"))
                            .collect::<Vec<_>>()
                            .join(",")
                    };
                    writeln!(
                        out,
                        "SNAPSHOT {} recv={} exch={} bids=[{}] asks=[{}]",
                        snapshot.symbol,
                        snapshot.received_at.to_rfc3339(),
                        snapshot.exchange_timestamp.to_rfc3339(),
                        levels(
                            snapshot
                                .bids
                                .iter()
                                .map(|l| (l.price_level, l.quantity))
                                .collect()
                        ),
                        levels(
                            snapshot
                                .asks
                                .iter()
                                .map(|l| (l.price_level, l.quantity))
                                .collect()
                        ),
                    )?;
                }
                let bids: Vec<_> = snapshot
                    .bids
                    .iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                let asks: Vec<_> = snapshot
                    .asks
                    .iter()
                    .map(|l| (l.price_level, l.quantity))
                    .collect();
                books.insert(snapshot.symbol, OrderBook::from_levels(&bids, &asks));
            }
            AnyMessage::PriceLevelUpdate(decoder) => {
                let update = PriceLevelUpdateDomain::try_from_decoder(decoder)?;
                summary.updates += 1;
                if !quiet {
                    writeln!(
                        out,
                        "UPDATE {} recv={} exch={} {:?} {}@{}",
                        update.symbol,
                        update.backtest_timestamp.to_rfc3339(),
                        update.exchange_timestamp.to_rfc3339(),
                        update.side,
                        update.quantity,
                        update.price_level,
                    )?;
                }
                let side = match update.side {
                    BookSide::Bid => Side::Bid,
                    _ => Side::Ask,
                };
                match books.get_mut(&update.symbol) {
                    Some(book) => {
                        let _ =
                            book.apply(side, update.price_level, LevelChange::Set(update.quantity));
                        if book.is_crossed() {
                            summary.crossed_after_update += 1;
                        }
                    }
                    None => summary.updates_without_snapshot += 1,
                }
            }
            _ => anyhow::bail!("unknown message"),
        }
    }
    summary.symbols = books.len();
    summary.books = books;
    Ok(summary)
}
