//! Kalshi `orderbook_delta` channel.
//!
//! * Discovery: `GET /trade-api/v2/markets?status=open&mve_filter=exclude`,
//!   which excludes multivariate (MVE) markets. The symbol is the market ticker.
//! * Stream: `wss://api.elections.kalshi.com/trade-api/ws/v2`, authenticated
//!   with an RSA-PSS signature. We subscribe one ticker per subscription so a
//!   sequence gap identifies exactly which book needs a resync.
//!
//! Kalshi books only contain bids for YES and bids for NO. A NO bid at price
//! `p` is equivalent to a YES ask at `1 - p`, so we publish every book from the
//! YES perspective: YES bids are bids, NO bids become asks at `1 - p`.

use std::collections::{HashMap, HashSet};
use std::path::Path;
use std::time::Duration;

use anyhow::{Context, bail};
use base64::Engine as _;
use chrono::{DateTime, Utc};
use rsa::RsaPrivateKey;
use rsa::pkcs1::DecodeRsaPrivateKey;
use rsa::pkcs8::DecodePrivateKey;
use rsa::pss::BlindedSigningKey;
use rsa::signature::{RandomizedSigner, SignatureEncoding};
use serde::Deserialize;
use serde_json::json;
use serde_json::value::RawValue;
use sha2::Sha256;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tokio_tungstenite::tungstenite::http::HeaderValue;
use tracing::{debug, warn};

use super::{Codec, Discovered, Event, Exchange, Keepalive, millis_to_datetime};
use crate::book::{LevelChange, Side};
use crate::fixed::{self, SCALE};

pub const PROD_REST_URL: &str = "https://api.elections.kalshi.com";
pub const PROD_WS_URL: &str = "wss://api.elections.kalshi.com/trade-api/ws/v2";
pub const DEMO_REST_URL: &str = "https://demo-api.kalshi.co";
pub const DEMO_WS_URL: &str = "wss://demo-api.kalshi.co/trade-api/ws/v2";

const PAGE_SIZE: usize = 1000;
const CHANNEL: &str = "orderbook_delta";

pub struct KalshiAuth {
    key_id: String,
    signing_key: BlindedSigningKey<Sha256>,
}

impl KalshiAuth {
    pub fn from_pem_file(key_id: String, path: &Path) -> anyhow::Result<Self> {
        let pem = std::fs::read_to_string(path)
            .with_context(|| format!("reading Kalshi private key {}", path.display()))?;
        let key = RsaPrivateKey::from_pkcs1_pem(&pem)
            .or_else(|_| RsaPrivateKey::from_pkcs8_pem(&pem))
            .context("Kalshi private key must be an RSA key in PKCS#1 or PKCS#8 PEM format")?;
        Ok(Self {
            key_id,
            signing_key: BlindedSigningKey::new(key),
        })
    }

    /// Signs `timestamp + method + path` with RSA-PSS (SHA-256, salt length =
    /// digest length), as Kalshi requires.
    fn sign(&self, message: &str) -> String {
        let signature = self
            .signing_key
            .sign_with_rng(&mut rand::thread_rng(), message.as_bytes());
        base64::engine::general_purpose::STANDARD.encode(signature.to_bytes())
    }
}

pub struct Kalshi {
    http: reqwest::Client,
    rest_url: String,
    ws_url: String,
    auth: KalshiAuth,
}

impl Kalshi {
    pub fn new(rest_url: String, ws_url: String, auth: KalshiAuth) -> anyhow::Result<Self> {
        super::install_crypto_provider();
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            rest_url,
            ws_url,
            auth,
        })
    }

    async fn fetch_page(&self, cursor: &str) -> anyhow::Result<MarketsPage> {
        let url = format!(
            "{}/trade-api/v2/markets",
            self.rest_url.trim_end_matches('/')
        );
        let mut attempt = 0;
        loop {
            let mut query = vec![
                ("status", "open".to_owned()),
                ("mve_filter", "exclude".to_owned()),
                ("limit", PAGE_SIZE.to_string()),
            ];
            if !cursor.is_empty() {
                query.push(("cursor", cursor.to_owned()));
            }
            let result = async {
                self.http
                    .get(&url)
                    .query(&query)
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<MarketsPage>()
                    .await
            }
            .await;
            match result {
                Ok(page) => return Ok(page),
                Err(error) if attempt < 3 => {
                    attempt += 1;
                    warn!(%error, attempt, "kalshi market page failed, retrying");
                    tokio::time::sleep(Duration::from_millis(500 << attempt)).await;
                }
                Err(error) => return Err(error).context("fetching kalshi markets"),
            }
        }
    }
}

#[derive(Deserialize)]
struct MarketsPage {
    #[serde(default)]
    markets: Vec<Market>,
    cursor: Option<String>,
}

#[derive(Deserialize)]
struct Market {
    ticker: String,
    mve_collection_ticker: Option<String>,
}

impl Exchange for Kalshi {
    type Codec = KalshiCodec;

    fn name(&self) -> &'static str {
        "kalshi"
    }

    async fn discover(&self) -> anyhow::Result<Discovered> {
        let mut tickers = HashSet::new();
        let mut cursor = String::new();
        let mut complete = true;
        loop {
            let page = match self.fetch_page(&cursor).await {
                Ok(page) => page,
                Err(error) if !cursor.is_empty() => {
                    warn!(error = %format!("{error:#}"), "kalshi listing incomplete");
                    complete = false;
                    break;
                }
                Err(error) => return Err(error),
            };
            tickers.extend(
                page.markets
                    .into_iter()
                    .filter(|m| m.mve_collection_ticker.as_deref().is_none_or(str::is_empty))
                    .map(|m| m.ticker),
            );
            match page.cursor {
                Some(next) if !next.is_empty() && next != cursor => cursor = next,
                _ => break,
            }
            // Stay well inside the public REST rate limit.
            tokio::time::sleep(Duration::from_millis(100)).await;
        }
        let mut symbols: Vec<_> = tickers.into_iter().collect();
        symbols.sort_unstable();
        Ok(Discovered { symbols, complete })
    }

    fn connect_request(&self) -> anyhow::Result<Request> {
        let mut request = self.ws_url.as_str().into_client_request()?;
        let timestamp = Utc::now().timestamp_millis().to_string();
        let path = request.uri().path().to_owned();
        let signature = self.auth.sign(&format!("{timestamp}GET{path}"));
        let headers = request.headers_mut();
        headers.insert(
            "KALSHI-ACCESS-KEY",
            HeaderValue::from_str(&self.auth.key_id)?,
        );
        headers.insert(
            "KALSHI-ACCESS-TIMESTAMP",
            HeaderValue::from_str(&timestamp)?,
        );
        headers.insert(
            "KALSHI-ACCESS-SIGNATURE",
            HeaderValue::from_str(&signature)?,
        );
        Ok(request)
    }

    fn new_codec(&self) -> KalshiCodec {
        KalshiCodec::default()
    }
}

struct Subscription {
    ticker: String,
    /// `None` until the snapshot arrives, and again after a sequence gap.
    last_seq: Option<u64>,
}

#[derive(Default)]
pub struct KalshiCodec {
    next_id: u64,
    /// Command id → ticker, for subscribe commands awaiting confirmation.
    pending: HashMap<u64, String>,
    /// Subscribe commands we no longer want; cancel them once confirmed.
    cancelled: HashMap<u64, String>,
    sids: HashMap<u64, Subscription>,
    ticker_sid: HashMap<String, u64>,
    outbox: Vec<String>,
}

#[derive(Deserialize)]
struct Envelope<'a> {
    #[serde(rename = "type")]
    kind: String,
    id: Option<u64>,
    sid: Option<u64>,
    seq: Option<u64>,
    #[serde(borrow)]
    msg: Option<&'a RawValue>,
}

#[derive(Deserialize)]
struct Subscribed {
    sid: u64,
}

#[derive(Deserialize)]
struct ErrorBody {
    code: Option<i64>,
    msg: Option<String>,
}

/// Kalshi has moved between integer and string encodings for numbers.
#[derive(Deserialize, Debug)]
#[serde(untagged)]
enum Num {
    Int(i64),
    Text(String),
    Float(f64),
}

impl Num {
    /// A price in dollars (`"0.5600"`) as fixed point.
    fn dollars(&self) -> anyhow::Result<i64> {
        match self {
            Num::Int(dollars) => dollars.checked_mul(SCALE).context("price out of range"),
            Num::Text(text) => fixed::parse(text),
            Num::Float(value) => fixed::parse(&format!("{value}")),
        }
    }

    /// A legacy price in integer cents as fixed point.
    fn cents(&self) -> anyhow::Result<i64> {
        match self {
            Num::Int(cents) => cents.checked_mul(SCALE / 100).context("price out of range"),
            other => Ok(other.dollars()? / 100),
        }
    }

    /// A contract count (`100` or `"100.00"`) as fixed point.
    fn contracts(&self) -> anyhow::Result<i64> {
        self.dollars()
    }
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Ts {
    Millis(i64),
    Iso(String),
}

impl Ts {
    fn to_datetime(&self) -> Option<DateTime<Utc>> {
        match self {
            Ts::Millis(ms) => millis_to_datetime(*ms),
            Ts::Iso(text) => DateTime::parse_from_rfc3339(text)
                .ok()
                .map(|ts| ts.with_timezone(&Utc)),
        }
    }
}

type RawLevels = Vec<(Num, Num)>;

#[derive(Deserialize)]
struct SnapshotBody {
    market_ticker: String,
    yes_dollars_fp: Option<RawLevels>,
    yes_dollars: Option<RawLevels>,
    yes: Option<RawLevels>,
    no_dollars_fp: Option<RawLevels>,
    no_dollars: Option<RawLevels>,
    no: Option<RawLevels>,
    ts: Option<Ts>,
    ts_ms: Option<i64>,
}

#[derive(Deserialize)]
struct DeltaBody {
    price_dollars: Option<Num>,
    price: Option<Num>,
    delta_fp: Option<Num>,
    delta: Option<Num>,
    side: String,
    ts: Option<Ts>,
    ts_ms: Option<i64>,
}

fn exchange_ts(ts: Option<&Ts>, ts_ms: Option<i64>) -> Option<DateTime<Utc>> {
    ts_ms
        .and_then(millis_to_datetime)
        .or_else(|| ts.and_then(Ts::to_datetime))
}

/// Picks the newest encoding present and converts it to `(price, qty)` in the
/// YES-relative frame for `side`.
fn levels(
    dollars_fp: Option<RawLevels>,
    dollars: Option<RawLevels>,
    cents: Option<RawLevels>,
    no_side: bool,
) -> anyhow::Result<Vec<(i64, i64)>> {
    let (raw, in_cents) = match (dollars_fp, dollars, cents) {
        (Some(raw), _, _) | (None, Some(raw), _) => (raw, false),
        (None, None, Some(raw)) => (raw, true),
        (None, None, None) => return Ok(Vec::new()),
    };
    raw.iter()
        .map(|(price, qty)| {
            let price = if in_cents {
                price.cents()?
            } else {
                price.dollars()?
            };
            let price = if no_side { SCALE - price } else { price };
            Ok((price, qty.contracts()?))
        })
        .collect()
}

impl KalshiCodec {
    fn subscription_for(&mut self, sid: Option<u64>, ticker: &str) -> Option<&mut Subscription> {
        let sid = sid?;
        if !self.sids.contains_key(&sid) {
            // A snapshot can in principle race the "subscribed" confirmation.
            // If an older subscribe for this ticker was cancelled, we can't
            // tell which command this sid belongs to: drop the snapshot and
            // wait for the confirmations (the engine retries if needed).
            if self.cancelled.values().any(|cancelled| cancelled == ticker) {
                return None;
            }
            let id = self
                .pending
                .iter()
                .find_map(|(id, pending)| (pending == ticker).then_some(*id))?;
            self.pending.remove(&id);
            self.register(sid, ticker.to_owned());
        }
        self.sids.get_mut(&sid)
    }

    fn register(&mut self, sid: u64, ticker: String) {
        if let Some(old) = self.ticker_sid.insert(ticker.clone(), sid)
            && old != sid
        {
            self.sids.remove(&old);
        }
        self.sids.insert(
            sid,
            Subscription {
                ticker,
                last_seq: None,
            },
        );
    }

    fn command(&mut self, cmd: &str, params: serde_json::Value) -> (u64, String) {
        self.next_id += 1;
        let id = self.next_id;
        (
            id,
            json!({ "id": id, "cmd": cmd, "params": params }).to_string(),
        )
    }

    fn handle(&mut self, envelope: Envelope<'_>, out: &mut Vec<Event>) -> anyhow::Result<()> {
        let body = envelope.msg.map(RawValue::get).unwrap_or("null");
        match envelope.kind.as_str() {
            "subscribed" => {
                let Subscribed { sid } = serde_json::from_str(body)?;
                let Some(id) = envelope.id else { return Ok(()) };
                if self.cancelled.remove(&id).is_some() {
                    let (_, message) = self.command("unsubscribe", json!({ "sids": [sid] }));
                    self.outbox.push(message);
                } else if let Some(ticker) = self.pending.remove(&id) {
                    self.register(sid, ticker);
                }
            }
            "orderbook_snapshot" => {
                let snapshot: SnapshotBody = serde_json::from_str(body)?;
                let ts = exchange_ts(snapshot.ts.as_ref(), snapshot.ts_ms);
                let ticker = snapshot.market_ticker;
                let Some(subscription) = self.subscription_for(envelope.sid, &ticker) else {
                    return Ok(());
                };
                subscription.last_seq = envelope.seq;
                let parsed = levels(
                    snapshot.yes_dollars_fp,
                    snapshot.yes_dollars,
                    snapshot.yes,
                    false,
                )
                .and_then(|bids| {
                    let asks = levels(
                        snapshot.no_dollars_fp,
                        snapshot.no_dollars,
                        snapshot.no,
                        true,
                    )?;
                    Ok((bids, asks))
                });
                match parsed {
                    Ok((bids, asks)) => out.push(Event::Snapshot {
                        symbol: ticker,
                        bids,
                        asks,
                        exchange_ts: ts,
                    }),
                    Err(error) => {
                        subscription.last_seq = None;
                        out.push(Event::Desync {
                            symbol: ticker,
                            reason: format!("unparseable snapshot: {error:#}"),
                        });
                    }
                }
            }
            "orderbook_delta" => {
                let delta: DeltaBody = serde_json::from_str(body)?;
                let Some(sid) = envelope.sid else {
                    return Ok(());
                };
                let Some(subscription) = self.sids.get_mut(&sid) else {
                    return Ok(());
                };
                // Deltas before the snapshot (or after a gap) are meaningless.
                let Some(last_seq) = subscription.last_seq else {
                    return Ok(());
                };
                let ticker = subscription.ticker.clone();
                if let Some(seq) = envelope.seq {
                    if seq != last_seq + 1 {
                        subscription.last_seq = None;
                        out.push(Event::Desync {
                            symbol: ticker,
                            reason: format!("sequence gap: expected {}, got {seq}", last_seq + 1),
                        });
                        return Ok(());
                    }
                    subscription.last_seq = Some(seq);
                }
                match parse_delta(&delta) {
                    Ok((side, price, change)) => out.push(Event::Delta {
                        symbol: ticker,
                        side,
                        price,
                        change,
                        exchange_ts: exchange_ts(delta.ts.as_ref(), delta.ts_ms),
                    }),
                    Err(error) => {
                        subscription.last_seq = None;
                        out.push(Event::Desync {
                            symbol: ticker,
                            reason: format!("unparseable delta: {error:#}"),
                        });
                    }
                }
            }
            "error" => {
                let error: ErrorBody = serde_json::from_str(body).unwrap_or(ErrorBody {
                    code: None,
                    msg: Some(body.to_owned()),
                });
                if let Some(id) = envelope.id {
                    self.cancelled.remove(&id);
                }
                let ticker = envelope.id.and_then(|id| self.pending.remove(&id));
                warn!(code = ?error.code, msg = ?error.msg, ?ticker, "kalshi command error");
                if let Some(ticker) = ticker {
                    // Let the engine retry the subscription after its backoff.
                    out.push(Event::Desync {
                        symbol: ticker,
                        reason: format!("subscribe failed: {:?}", error.msg),
                    });
                }
            }
            "unsubscribed" | "ok" => {}
            other => debug!(kind = other, "ignoring kalshi message"),
        }
        Ok(())
    }
}

fn parse_delta(delta: &DeltaBody) -> anyhow::Result<(Side, i64, LevelChange)> {
    let price = match (&delta.price_dollars, &delta.price) {
        (Some(dollars), _) => dollars.dollars()?,
        (None, Some(cents)) => cents.cents()?,
        (None, None) => bail!("missing price"),
    };
    let change = delta
        .delta_fp
        .as_ref()
        .or(delta.delta.as_ref())
        .context("missing delta")?
        .contracts()?;
    match delta.side.as_str() {
        "yes" => Ok((Side::Bid, price, LevelChange::Add(change))),
        "no" => Ok((Side::Ask, SCALE - price, LevelChange::Add(change))),
        other => bail!("unknown side `{other}`"),
    }
}

impl Codec for KalshiCodec {
    fn subscribe(&mut self, symbols: &[String]) -> Vec<String> {
        symbols
            .iter()
            .map(|ticker| {
                let (id, message) = self.command(
                    "subscribe",
                    json!({ "channels": [CHANNEL], "market_ticker": ticker }),
                );
                self.pending.insert(id, ticker.clone());
                message
            })
            .collect()
    }

    fn unsubscribe(&mut self, symbols: &[String]) -> Vec<String> {
        let mut sids = Vec::new();
        for ticker in symbols {
            if let Some(sid) = self.ticker_sid.remove(ticker) {
                self.sids.remove(&sid);
                sids.push(sid);
            }
            let stale: Vec<u64> = self
                .pending
                .iter()
                .filter_map(|(id, pending)| (pending == ticker).then_some(*id))
                .collect();
            for id in stale {
                self.pending.remove(&id);
                self.cancelled.insert(id, ticker.clone());
            }
        }
        if sids.is_empty() {
            return Vec::new();
        }
        vec![self.command("unsubscribe", json!({ "sids": sids })).1]
    }

    fn keepalive(&self) -> Keepalive {
        Keepalive::PingFrame
    }

    fn decode(&mut self, text: &str, out: &mut Vec<Event>) -> anyhow::Result<()> {
        let envelope: Envelope<'_> = serde_json::from_str(text)?;
        self.handle(envelope, out)
    }

    fn drain_outbox(&mut self) -> Vec<String> {
        std::mem::take(&mut self.outbox)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(codec: &mut KalshiCodec, text: &str) -> Vec<Event> {
        let mut out = Vec::new();
        codec.decode(text, &mut out).unwrap();
        out
    }

    fn subscribed_codec() -> KalshiCodec {
        let mut codec = KalshiCodec::default();
        let messages = codec.subscribe(&["T1".into()]);
        let sent: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
        assert_eq!(sent["cmd"], "subscribe");
        assert_eq!(sent["params"]["market_ticker"], "T1");
        assert!(
            decode(
                &mut codec,
                r#"{"id":1,"type":"subscribed","msg":{"channel":"orderbook_delta","sid":7}}"#
            )
            .is_empty()
        );
        codec
    }

    #[test]
    fn snapshot_maps_no_side_to_asks() {
        let mut codec = subscribed_codec();
        let events = decode(
            &mut codec,
            r#"{"type":"orderbook_snapshot","sid":7,"seq":1,"msg":{"market_ticker":"T1",
                "yes_dollars_fp":[["0.4000","100.00"],["0.4500","5.00"]],
                "no_dollars_fp":[["0.5000","20.00"]]}}"#,
        );
        assert_eq!(
            events,
            vec![Event::Snapshot {
                symbol: "T1".into(),
                bids: vec![(400_000, 100_000_000), (450_000, 5_000_000)],
                asks: vec![(500_000, 20_000_000)],
                exchange_ts: None,
            }]
        );
    }

    #[test]
    fn legacy_cent_snapshot() {
        let mut codec = subscribed_codec();
        let events = decode(
            &mut codec,
            r#"{"type":"orderbook_snapshot","sid":7,"seq":1,"msg":{"market_ticker":"T1",
                "yes":[[40,100]],"no":[[45,3]]}}"#,
        );
        assert_eq!(
            events,
            vec![Event::Snapshot {
                symbol: "T1".into(),
                bids: vec![(400_000, 100_000_000)],
                asks: vec![(550_000, 3_000_000)],
                exchange_ts: None,
            }]
        );
    }

    #[test]
    fn deltas_and_sequence_gaps() {
        let mut codec = subscribed_codec();
        decode(
            &mut codec,
            r#"{"type":"orderbook_snapshot","sid":7,"seq":1,"msg":{"market_ticker":"T1","yes_dollars_fp":[],"no_dollars_fp":[]}}"#,
        );
        let events = decode(
            &mut codec,
            r#"{"type":"orderbook_delta","sid":7,"seq":2,"msg":{"market_ticker":"T1",
                "price_dollars":"0.9600","delta_fp":"-54.00","side":"no","ts":"2026-04-22T18:31:59.043421Z"}}"#,
        );
        assert_eq!(
            events,
            vec![Event::Delta {
                symbol: "T1".into(),
                side: Side::Ask,
                price: 40_000,
                change: LevelChange::Add(-54_000_000),
                exchange_ts: Some("2026-04-22T18:31:59.043421Z".parse().unwrap()),
            }]
        );

        let gap = decode(
            &mut codec,
            r#"{"type":"orderbook_delta","sid":7,"seq":4,"msg":{"market_ticker":"T1","price_dollars":"0.5","delta_fp":"1","side":"yes"}}"#,
        );
        assert!(matches!(&gap[..], [Event::Desync { symbol, .. }] if symbol == "T1"));
        // Everything after a gap is ignored until a new snapshot arrives.
        let after = decode(
            &mut codec,
            r#"{"type":"orderbook_delta","sid":7,"seq":5,"msg":{"market_ticker":"T1","price_dollars":"0.5","delta_fp":"1","side":"yes"}}"#,
        );
        assert!(after.is_empty());
    }

    #[test]
    fn unsubscribe_and_late_confirmation() {
        let mut codec = subscribed_codec();
        let messages = codec.unsubscribe(&["T1".into()]);
        let sent: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
        assert_eq!(sent["params"]["sids"], json!([7]));

        // Subscribe then cancel before the exchange confirms.
        codec.subscribe(&["T2".into()]);
        let id = codec.next_id;
        assert!(codec.unsubscribe(&["T2".into()]).is_empty());
        decode(
            &mut codec,
            &format!(
                r#"{{"id":{id},"type":"subscribed","msg":{{"channel":"orderbook_delta","sid":9}}}}"#
            ),
        );
        let outbox = codec.drain_outbox();
        let sent: serde_json::Value = serde_json::from_str(&outbox[0]).unwrap();
        assert_eq!(sent["params"]["sids"], json!([9]));
    }

    #[test]
    fn signs_requests() {
        use rsa::pkcs8::EncodePrivateKey;
        let key = RsaPrivateKey::new(&mut rand::thread_rng(), 1024).unwrap();
        let pem = key.to_pkcs8_pem(rsa::pkcs8::LineEnding::LF).unwrap();
        let file = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(file.path(), pem.as_bytes()).unwrap();
        let auth = KalshiAuth::from_pem_file("key".into(), file.path()).unwrap();
        let kalshi = Kalshi::new(PROD_REST_URL.into(), PROD_WS_URL.into(), auth).unwrap();
        let request = kalshi.connect_request().unwrap();
        assert_eq!(request.headers()["KALSHI-ACCESS-KEY"], "key");
        assert!(!request.headers()["KALSHI-ACCESS-SIGNATURE"].is_empty());
    }
}
