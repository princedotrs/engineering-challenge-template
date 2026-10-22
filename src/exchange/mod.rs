//! The exchange-specific layer.
//!
//! Everything exchange-specific lives behind two traits: [`Exchange`] (how to
//! discover symbols and open a websocket) and [`Codec`] (how to encode
//! subscriptions and decode messages on one websocket). The rest of the
//! service — sharding, redundancy, monitoring, book building and output — is
//! generic and never looks at exchange JSON.

pub mod kalshi;
pub mod polymarket;

use std::future::Future;

use chrono::{DateTime, Utc};
use tokio_tungstenite::tungstenite::handshake::client::Request;

use crate::book::{LevelChange, Side};

/// A normalised message decoded from an exchange websocket.
#[derive(Clone, Debug, PartialEq)]
pub enum Event {
    /// The full book for a symbol. Replaces whatever we had.
    Snapshot {
        symbol: String,
        bids: Vec<(i64, i64)>,
        asks: Vec<(i64, i64)>,
        exchange_ts: Option<DateTime<Utc>>,
    },
    /// A change to a single price level.
    Delta {
        symbol: String,
        side: Side,
        price: i64,
        change: LevelChange,
        exchange_ts: Option<DateTime<Utc>>,
    },
    /// The exchange's own view of the top of book, used to verify ours.
    /// `None` means that side is empty.
    TopOfBook {
        symbol: String,
        best_bid: Option<i64>,
        best_ask: Option<i64>,
    },
    /// The codec detected that it can no longer trust this symbol's stream on
    /// this connection (for example a sequence gap). The symbol must be
    /// resubscribed to get a fresh snapshot.
    Desync { symbol: String, reason: String },
}

/// The result of a symbol discovery.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct Discovered {
    pub symbols: Vec<String>,
    /// `false` if listing stopped early (e.g. a page failed). An incomplete
    /// listing is only used to add symbols, never to remove them.
    pub complete: bool,
}

impl Discovered {
    pub fn complete(symbols: Vec<String>) -> Self {
        Self {
            symbols,
            complete: true,
        }
    }
}

/// How a connection proves it is alive.
#[derive(Clone, Copy, Debug)]
pub enum Keepalive {
    /// Send this text message (e.g. Polymarket's `PING`).
    Text(&'static str),
    /// Send a websocket ping frame.
    PingFrame,
}

/// Exchange-level operations, shared by every connection.
pub trait Exchange: Send + Sync + 'static {
    type Codec: Codec;

    fn name(&self) -> &'static str;

    /// Returns every symbol that is currently trading.
    fn discover(&self) -> impl Future<Output = anyhow::Result<Discovered>> + Send;

    /// Builds the websocket handshake request, including any auth headers.
    /// Called for every (re)connect, so signatures are always fresh.
    fn connect_request(&self) -> anyhow::Result<Request>;

    /// Creates the per-connection codec. A new codec is created for every
    /// (re)connect, so codec state never leaks across sockets.
    fn new_codec(&self) -> Self::Codec;
}

/// Per-connection encoding and decoding.
pub trait Codec: Send + 'static {
    /// Messages to send to subscribe to `symbols`.
    fn subscribe(&mut self, symbols: &[String]) -> Vec<String>;

    /// Messages to send to unsubscribe from `symbols`.
    fn unsubscribe(&mut self, symbols: &[String]) -> Vec<String>;

    fn keepalive(&self) -> Keepalive;

    /// Decodes one text frame, appending events to `out`.
    fn decode(&mut self, text: &str, out: &mut Vec<Event>) -> anyhow::Result<()>;

    /// Messages the codec wants sent in reaction to what it decoded (for
    /// example cancelling a subscription that was confirmed after we stopped
    /// wanting it). Drained after every decoded frame.
    fn drain_outbox(&mut self) -> Vec<String> {
        Vec::new()
    }
}

/// TLS for both websockets and REST uses rustls with the `ring` provider,
/// which avoids a C toolchain dependency. Safe to call repeatedly.
pub fn install_crypto_provider() {
    let _ = rustls::crypto::ring::default_provider().install_default();
}

/// Parses an exchange timestamp in milliseconds since the Unix epoch.
pub(crate) fn millis_to_datetime(ms: i64) -> Option<DateTime<Utc>> {
    DateTime::from_timestamp_millis(ms)
}
