//! An in-memory L2 order book for a single symbol.

use std::collections::BTreeMap;

use chrono::{DateTime, Utc};

use crate::util::dtos::challenge_orderbook::{
    SnapshotAsksEntryDomain, SnapshotBidsEntryDomain, SnapshotDomain,
};

/// Side of the book a price level belongs to.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum Side {
    Bid,
    Ask,
}

/// How a price level changes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum LevelChange {
    /// The new absolute quantity at the level (Polymarket style).
    Set(i64),
    /// A signed change to the quantity at the level (Kalshi style).
    Add(i64),
}

/// A price level update could not be applied consistently.
#[derive(Debug, PartialEq, Eq)]
pub struct NegativeQuantity;

/// Price levels in fixed point, keyed by price. A level with zero quantity is
/// never stored.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct OrderBook {
    bids: BTreeMap<i64, i64>,
    asks: BTreeMap<i64, i64>,
    /// Exchange timestamp of the latest change applied to this book.
    pub last_exchange_ts: Option<DateTime<Utc>>,
}

impl OrderBook {
    pub fn from_levels(bids: &[(i64, i64)], asks: &[(i64, i64)]) -> Self {
        let collect = |levels: &[(i64, i64)]| {
            levels
                .iter()
                .filter(|(_, qty)| *qty > 0)
                .copied()
                .collect::<BTreeMap<_, _>>()
        };
        Self {
            bids: collect(bids),
            asks: collect(asks),
            last_exchange_ts: None,
        }
    }

    fn side_mut(&mut self, side: Side) -> &mut BTreeMap<i64, i64> {
        match side {
            Side::Bid => &mut self.bids,
            Side::Ask => &mut self.asks,
        }
    }

    /// Applies a change and returns the new absolute quantity at the level
    /// (`0` means the level was removed).
    pub fn apply(
        &mut self,
        side: Side,
        price: i64,
        change: LevelChange,
    ) -> Result<i64, NegativeQuantity> {
        let levels = self.side_mut(side);
        let current = levels.get(&price).copied().unwrap_or(0);
        let next = match change {
            LevelChange::Set(qty) => qty,
            LevelChange::Add(delta) => current.saturating_add(delta),
        };
        if next < 0 {
            return Err(NegativeQuantity);
        }
        if next == 0 {
            levels.remove(&price);
        } else {
            levels.insert(price, next);
        }
        Ok(next)
    }

    pub fn best_bid(&self) -> Option<i64> {
        self.bids.keys().next_back().copied()
    }

    pub fn best_ask(&self) -> Option<i64> {
        self.asks.keys().next().copied()
    }

    /// A crossed book is a sign we missed or misapplied an update.
    pub fn is_crossed(&self) -> bool {
        matches!((self.best_bid(), self.best_ask()), (Some(bid), Some(ask)) if bid >= ask)
    }

    pub fn level_count(&self) -> usize {
        self.bids.len() + self.asks.len()
    }

    /// Every level change needed to turn `self` into `other`, as
    /// `(side, price, new absolute quantity)`.
    pub fn diff(&self, other: &OrderBook) -> Vec<(Side, i64, i64)> {
        let mut changes = Vec::new();
        for (side, ours, theirs) in [
            (Side::Bid, &self.bids, &other.bids),
            (Side::Ask, &self.asks, &other.asks),
        ] {
            for price in ours.keys() {
                if !theirs.contains_key(price) {
                    changes.push((side, *price, 0));
                }
            }
            for (price, qty) in theirs {
                if ours.get(price) != Some(qty) {
                    changes.push((side, *price, *qty));
                }
            }
        }
        changes
    }

    pub fn to_snapshot(&self, symbol: &str, received_at: DateTime<Utc>) -> SnapshotDomain {
        SnapshotDomain {
            received_at,
            exchange_timestamp: self.last_exchange_ts.unwrap_or(received_at),
            // Best price first on both sides.
            bids: self
                .bids
                .iter()
                .rev()
                .map(|(&price_level, &quantity)| SnapshotBidsEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            asks: self
                .asks
                .iter()
                .map(|(&price_level, &quantity)| SnapshotAsksEntryDomain {
                    price_level,
                    quantity,
                })
                .collect(),
            symbol: symbol.to_owned(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn set_and_add_changes() {
        let mut book = OrderBook::default();
        assert_eq!(book.apply(Side::Bid, 500, LevelChange::Set(10)), Ok(10));
        assert_eq!(book.apply(Side::Bid, 500, LevelChange::Add(-4)), Ok(6));
        assert_eq!(book.apply(Side::Bid, 500, LevelChange::Add(-6)), Ok(0));
        assert_eq!(book.best_bid(), None);
        assert_eq!(
            book.apply(Side::Ask, 600, LevelChange::Add(-1)),
            Err(NegativeQuantity)
        );
    }

    #[test]
    fn best_prices_and_crossing() {
        let mut book = OrderBook::from_levels(&[(400, 1), (450, 2)], &[(600, 1), (550, 3)]);
        assert_eq!(book.best_bid(), Some(450));
        assert_eq!(book.best_ask(), Some(550));
        assert!(!book.is_crossed());
        book.apply(Side::Bid, 560, LevelChange::Set(1)).unwrap();
        assert!(book.is_crossed());
    }

    #[test]
    fn diff_reaches_target() {
        let old = OrderBook::from_levels(&[(400, 1), (450, 2)], &[(600, 1)]);
        let new = OrderBook::from_levels(&[(400, 5)], &[(600, 1), (650, 2)]);
        let mut patched = old.clone();
        for (side, price, qty) in old.diff(&new) {
            patched.apply(side, price, LevelChange::Set(qty)).unwrap();
        }
        assert_eq!(patched, new);
        assert_eq!(old.diff(&old), vec![]);
    }

    #[test]
    fn snapshot_orders_best_first() {
        let book = OrderBook::from_levels(&[(400, 1), (450, 2)], &[(600, 1), (550, 3)]);
        let snapshot = book.to_snapshot("X", DateTime::UNIX_EPOCH);
        assert_eq!(snapshot.bids[0].price_level, 450);
        assert_eq!(snapshot.asks[0].price_level, 550);
    }
}
