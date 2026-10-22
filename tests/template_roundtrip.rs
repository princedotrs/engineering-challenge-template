//! The template's original example, kept as a test: write a snapshot and an
//! update with the provided helpers and read them back unchanged.

use chrono::DateTime;
use engineering_challenge::util::dtos::challenge_orderbook::{
    AnyMessage, BookSide, PriceLevelUpdateDomain, SnapshotAsksEntryDomain, SnapshotBidsEntryDomain,
    SnapshotDomain,
};
use engineering_challenge::util::{AlignedSbeReader, AlignedSbeWriter, OrderbookSchema};

const CHUNK_SIZE_64_MEBIBYTES: usize = 64 << 20;

#[test]
fn snapshot_and_update_round_trip() -> anyhow::Result<()> {
    let timestamp = DateTime::from_timestamp_micros(1_700_000_000_000_000).unwrap();
    let symbol = "EXAMPLE".to_owned();
    let snapshot = SnapshotDomain {
        received_at: timestamp,
        exchange_timestamp: timestamp,
        bids: vec![SnapshotBidsEntryDomain {
            price_level: 400_000,
            quantity: 2_000_000,
        }],
        asks: vec![SnapshotAsksEntryDomain {
            price_level: 600_000,
            quantity: 3_500_000,
        }],
        symbol: symbol.clone(),
    };
    let update = PriceLevelUpdateDomain {
        backtest_timestamp: timestamp,
        exchange_timestamp: timestamp,
        price_level: 400_000,
        quantity: 1_250_000,
        side: BookSide::Bid,
        symbol: symbol.clone(),
    };

    let file = tempfile::NamedTempFile::new()?;
    let mut writer = AlignedSbeWriter::create(file.path(), CHUNK_SIZE_64_MEBIBYTES)?;
    writer.write_msg(&snapshot)?;
    writer.write_msg(&update)?;
    writer.flush()?;
    drop(writer);

    let mut reader = AlignedSbeReader::<OrderbookSchema>::open(file.path())?;
    let Some(AnyMessage::Snapshot(decoded)) = reader.read_msg()? else {
        anyhow::bail!("expected snapshot");
    };
    assert_eq!(SnapshotDomain::try_from_decoder(decoded)?, snapshot);
    let Some(AnyMessage::PriceLevelUpdate(decoded)) = reader.read_msg()? else {
        anyhow::bail!("expected price level update");
    };
    assert_eq!(PriceLevelUpdateDomain::try_from_decoder(decoded)?, update);
    assert!(reader.read_msg()?.is_none());
    Ok(())
}
