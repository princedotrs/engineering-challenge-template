//! Polymarket CLOB market channel.
//!
//! * Discovery: Gamma REST API (`/markets`), every open market with an order
//!   book. Each market has one token per outcome, and every token has its own
//!   book, so the symbol is the CLOB token id.
//! * Stream: `wss://ws-subscriptions-clob.polymarket.com/ws/market`. `book`
//!   messages are full snapshots, `price_change` messages carry the new
//!   absolute size at a level plus the exchange's best bid/ask after the
//!   change, which we use to verify our book.

use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Context;
use serde::Deserialize;
use serde_json::json;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::handshake::client::Request;
use tracing::{debug, warn};

use super::{Codec, Discovered, Event, Exchange, Keepalive, millis_to_datetime};
use crate::book::{LevelChange, Side};
use crate::fixed;

pub const DEFAULT_REST_URL: &str = "https://gamma-api.polymarket.com";
pub const DEFAULT_WS_URL: &str = "wss://ws-subscriptions-clob.polymarket.com/ws/market";

const PAGE_SIZE: usize = 500;
const MAX_MARKETS: usize = 1_000_000;

pub struct Polymarket {
    http: reqwest::Client,
    rest_url: String,
    ws_url: String,
    verify_top_of_book: bool,
}

impl Polymarket {
    pub fn new(rest_url: String, ws_url: String, verify_top_of_book: bool) -> anyhow::Result<Self> {
        super::install_crypto_provider();
        Ok(Self {
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(30))
                .build()?,
            rest_url,
            ws_url,
            verify_top_of_book,
        })
    }

    async fn fetch_page(&self, offset: usize) -> anyhow::Result<Vec<GammaMarket>> {
        let url = format!("{}/markets", self.rest_url.trim_end_matches('/'));
        let mut attempt = 0;
        loop {
            let result = async {
                self.http
                    .get(&url)
                    .query(&[
                        ("active", "true"),
                        ("closed", "false"),
                        ("limit", &PAGE_SIZE.to_string()),
                        ("offset", &offset.to_string()),
                    ])
                    .send()
                    .await?
                    .error_for_status()?
                    .json::<Vec<GammaMarket>>()
                    .await
            }
            .await;
            match result {
                Ok(page) => return Ok(page),
                Err(error) if attempt < 3 => {
                    attempt += 1;
                    warn!(%error, offset, attempt, "polymarket market page failed, retrying");
                    tokio::time::sleep(Duration::from_millis(500 << attempt)).await;
                }
                Err(error) => return Err(error).context("fetching polymarket markets"),
            }
        }
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct GammaMarket {
    clob_token_ids: Option<String>,
    enable_order_book: Option<bool>,
    accepting_orders: Option<bool>,
    active: Option<bool>,
    closed: Option<bool>,
}

impl GammaMarket {
    fn live_tokens(&self) -> Vec<String> {
        let live = self.enable_order_book == Some(true)
            && self.active != Some(false)
            && self.closed != Some(true)
            && self.accepting_orders != Some(false);
        if !live {
            return Vec::new();
        }
        // The token ids are a JSON array encoded inside a string.
        self.clob_token_ids
            .as_deref()
            .and_then(|ids| serde_json::from_str::<Vec<String>>(ids).ok())
            .unwrap_or_default()
    }
}

impl Exchange for Polymarket {
    type Codec = PolymarketCodec;

    fn name(&self) -> &'static str {
        "polymarket"
    }

    async fn discover(&self) -> anyhow::Result<Discovered> {
        let mut tokens = HashSet::new();
        let mut offset = 0;
        let mut complete = true;
        // Stop on an empty page rather than a short one, in case the API caps
        // the page size below what we asked for.
        loop {
            let page = match self.fetch_page(offset).await {
                Ok(page) => page,
                Err(error) if offset > 0 => {
                    warn!(error = %format!("{error:#}"), offset, "polymarket listing incomplete");
                    complete = false;
                    break;
                }
                Err(error) => return Err(error),
            };
            if page.is_empty() || offset > MAX_MARKETS {
                break;
            }
            offset += page.len();
            tokens.extend(page.iter().flat_map(GammaMarket::live_tokens));
        }
        let mut symbols: Vec<_> = tokens.into_iter().collect();
        symbols.sort_unstable();
        Ok(Discovered { symbols, complete })
    }

    fn connect_request(&self) -> anyhow::Result<Request> {
        Ok(self.ws_url.as_str().into_client_request()?)
    }

    fn new_codec(&self) -> PolymarketCodec {
        PolymarketCodec {
            verify_top_of_book: self.verify_top_of_book,
        }
    }
}

pub struct PolymarketCodec {
    verify_top_of_book: bool,
}

impl PolymarketCodec {
    pub fn new(verify_top_of_book: bool) -> Self {
        Self { verify_top_of_book }
    }
}

#[derive(Deserialize)]
#[serde(tag = "event_type")]
enum WsMessage {
    #[serde(rename = "book")]
    Book(BookMessage),
    #[serde(rename = "price_change")]
    PriceChange(PriceChangeMessage),
    #[serde(other)]
    Other,
}

#[derive(Deserialize)]
struct BookMessage {
    asset_id: String,
    timestamp: Option<Timestamp>,
    #[serde(default, alias = "buys")]
    bids: Vec<Level>,
    #[serde(default, alias = "sells")]
    asks: Vec<Level>,
}

#[derive(Deserialize)]
struct Level {
    price: String,
    size: String,
}

#[derive(Deserialize)]
struct PriceChangeMessage {
    timestamp: Option<Timestamp>,
    #[serde(default)]
    price_changes: Vec<serde_json::Value>,
    // Older message format: one asset per message with a `changes` list.
    asset_id: Option<String>,
    #[serde(default)]
    changes: Vec<serde_json::Value>,
}

#[derive(Deserialize)]
struct PriceChange {
    asset_id: Option<String>,
    price: String,
    size: Option<String>,
    side: String,
    best_bid: Option<String>,
    best_ask: Option<String>,
}

#[derive(Deserialize)]
#[serde(untagged)]
enum Timestamp {
    Text(String),
    Number(i64),
}

impl Timestamp {
    fn to_datetime(&self) -> Option<chrono::DateTime<chrono::Utc>> {
        match self {
            Timestamp::Text(text) => millis_to_datetime(text.parse().ok()?),
            Timestamp::Number(ms) => millis_to_datetime(*ms),
        }
    }
}

fn parse_levels(levels: &[Level]) -> anyhow::Result<Vec<(i64, i64)>> {
    levels
        .iter()
        .map(|level| Ok((fixed::parse(&level.price)?, fixed::parse(&level.size)?)))
        .collect()
}

/// Polymarket reports an empty side as a best price of 0 (or 1 for asks).
fn parse_best(text: Option<&str>, is_ask: bool) -> anyhow::Result<Option<i64>> {
    let Some(text) = text else { return Ok(None) };
    let price = fixed::parse(text)?;
    let empty = price <= 0 || (is_ask && price >= fixed::SCALE);
    Ok((!empty).then_some(price))
}

impl PolymarketCodec {
    fn handle(&self, message: WsMessage, out: &mut Vec<Event>) {
        match message {
            WsMessage::Book(book) => {
                let exchange_ts = book.timestamp.as_ref().and_then(Timestamp::to_datetime);
                match (parse_levels(&book.bids), parse_levels(&book.asks)) {
                    (Ok(bids), Ok(asks)) => out.push(Event::Snapshot {
                        symbol: book.asset_id,
                        bids,
                        asks,
                        exchange_ts,
                    }),
                    (Err(error), _) | (_, Err(error)) => out.push(Event::Desync {
                        symbol: book.asset_id,
                        reason: format!("unparseable book: {error:#}"),
                    }),
                }
            }
            WsMessage::PriceChange(message) => {
                let exchange_ts = message.timestamp.as_ref().and_then(Timestamp::to_datetime);
                // The exchange's best bid/ask after the last change to each asset.
                let mut tops: HashMap<String, (Option<String>, Option<String>)> = HashMap::new();
                let legacy_asset = message.asset_id;
                // Entries are decoded one by one, so a malformed entry only
                // affects its own asset (which gets resynced) and not the frame.
                for raw in message.price_changes.into_iter().chain(message.changes) {
                    let raw_asset = raw
                        .get("asset_id")
                        .and_then(serde_json::Value::as_str)
                        .map(str::to_owned)
                        .or_else(|| legacy_asset.clone());
                    let change = match serde_json::from_value::<PriceChange>(raw) {
                        Ok(change) => change,
                        Err(error) => {
                            if let Some(symbol) = raw_asset {
                                out.push(Event::Desync {
                                    symbol,
                                    reason: format!("malformed price change: {error}"),
                                });
                            }
                            continue;
                        }
                    };
                    let Some(symbol) = change.asset_id.or_else(|| legacy_asset.clone()) else {
                        continue;
                    };
                    let parsed = (|| -> anyhow::Result<(Side, i64, i64)> {
                        let side = match change.side.as_str() {
                            "BUY" | "buy" => Side::Bid,
                            "SELL" | "sell" => Side::Ask,
                            other => anyhow::bail!("unknown side `{other}`"),
                        };
                        let size = change.size.as_deref().context("missing size")?;
                        Ok((side, fixed::parse(&change.price)?, fixed::parse(size)?))
                    })();
                    match parsed {
                        Ok((side, price, size)) => out.push(Event::Delta {
                            symbol: symbol.clone(),
                            side,
                            price,
                            change: LevelChange::Set(size),
                            exchange_ts,
                        }),
                        Err(error) => {
                            out.push(Event::Desync {
                                symbol,
                                reason: format!("unparseable price change: {error:#}"),
                            });
                            continue;
                        }
                    }
                    if change.best_bid.is_some() && change.best_ask.is_some() {
                        tops.insert(symbol, (change.best_bid, change.best_ask));
                    }
                }
                if self.verify_top_of_book {
                    for (symbol, (bid, ask)) in tops {
                        match (
                            parse_best(bid.as_deref(), false),
                            parse_best(ask.as_deref(), true),
                        ) {
                            (Ok(best_bid), Ok(best_ask)) => out.push(Event::TopOfBook {
                                symbol,
                                best_bid,
                                best_ask,
                            }),
                            _ => debug!(%symbol, "unparseable best bid/ask"),
                        }
                    }
                }
            }
            WsMessage::Other => {}
        }
    }
}

impl Codec for PolymarketCodec {
    fn subscribe(&mut self, symbols: &[String]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        vec![
            json!({
                "type": "market",
                "operation": "subscribe",
                "markets": [],
                "assets_ids": symbols,
                "initial_dump": true,
            })
            .to_string(),
        ]
    }

    fn unsubscribe(&mut self, symbols: &[String]) -> Vec<String> {
        if symbols.is_empty() {
            return Vec::new();
        }
        vec![
            json!({
                "type": "market",
                "operation": "unsubscribe",
                "markets": [],
                "assets_ids": symbols,
            })
            .to_string(),
        ]
    }

    fn keepalive(&self) -> Keepalive {
        Keepalive::Text("PING")
    }

    fn decode(&mut self, text: &str, out: &mut Vec<Event>) -> anyhow::Result<()> {
        let trimmed = text.trim_start();
        if trimmed.starts_with('[') {
            let messages: Vec<serde_json::Value> = serde_json::from_str(trimmed)?;
            for message in messages {
                match serde_json::from_value::<WsMessage>(message) {
                    Ok(message) => self.handle(message, out),
                    Err(error) => debug!(%error, "skipping undecodable polymarket message"),
                }
            }
        } else if trimmed.starts_with('{') {
            self.handle(serde_json::from_str(trimmed)?, out);
        } else if trimmed != "PONG" {
            // Plain-text control replies such as "INVALID OPERATION".
            warn!(message = %trimmed, "polymarket text message");
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn decode(text: &str) -> Vec<Event> {
        let mut codec = PolymarketCodec {
            verify_top_of_book: true,
        };
        let mut out = Vec::new();
        codec.decode(text, &mut out).unwrap();
        out
    }

    #[test]
    fn decodes_book_array() {
        let events = decode(
            r#"[{"event_type":"book","asset_id":"123","market":"0xabc",
                "bids":[{"price":"0.48","size":"30"},{"price":"0.49","size":"20.5"}],
                "asks":[{"price":"0.52","size":"25"}],
                "timestamp":"1700000000123","hash":"0x0"}]"#,
        );
        assert_eq!(
            events,
            vec![Event::Snapshot {
                symbol: "123".into(),
                bids: vec![(480_000, 30_000_000), (490_000, 20_500_000)],
                asks: vec![(520_000, 25_000_000)],
                exchange_ts: millis_to_datetime(1_700_000_000_123),
            }]
        );
    }

    #[test]
    fn decodes_price_change_with_top_of_book() {
        let events = decode(
            r#"{"event_type":"price_change","market":"0xabc","timestamp":"1700000000500",
                "price_changes":[
                  {"asset_id":"123","price":"0.5","size":"200","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"0.52"},
                  {"asset_id":"123","price":"0.49","size":"0","side":"BUY","hash":"h","best_bid":"0.5","best_ask":"0.52"},
                  {"asset_id":"456","price":"0.51","size":"10","side":"SELL","hash":"h","best_bid":"0","best_ask":"0.51"}
                ]}"#,
        );
        let ts = millis_to_datetime(1_700_000_000_500);
        assert_eq!(
            events[..3],
            [
                Event::Delta {
                    symbol: "123".into(),
                    side: Side::Bid,
                    price: 500_000,
                    change: LevelChange::Set(200_000_000),
                    exchange_ts: ts,
                },
                Event::Delta {
                    symbol: "123".into(),
                    side: Side::Bid,
                    price: 490_000,
                    change: LevelChange::Set(0),
                    exchange_ts: ts,
                },
                Event::Delta {
                    symbol: "456".into(),
                    side: Side::Ask,
                    price: 510_000,
                    change: LevelChange::Set(10_000_000),
                    exchange_ts: ts,
                },
            ]
        );
        let mut tops = events[3..].to_vec();
        tops.sort_by_key(|event| format!("{event:?}"));
        assert_eq!(
            tops,
            vec![
                Event::TopOfBook {
                    symbol: "123".into(),
                    best_bid: Some(500_000),
                    best_ask: Some(520_000),
                },
                Event::TopOfBook {
                    symbol: "456".into(),
                    best_bid: None,
                    best_ask: Some(510_000),
                },
            ]
        );
    }

    #[test]
    fn malformed_entry_only_desyncs_its_asset() {
        let events = decode(
            r#"{"event_type":"price_change","market":"0xabc","timestamp":"1",
                "price_changes":[
                  {"asset_id":"1","price":null,"size":"1","side":"BUY"},
                  {"asset_id":"2","price":"0.4","size":"1","side":"BUY"}
                ]}"#,
        );
        assert!(matches!(&events[0], Event::Desync { symbol, .. } if symbol == "1"));
        assert!(matches!(&events[1], Event::Delta { symbol, .. } if symbol == "2"));
    }

    #[test]
    fn ignores_other_messages() {
        assert!(decode("PONG").is_empty());
        assert!(
            decode(r#"{"event_type":"last_trade_price","asset_id":"1","price":"0.5"}"#).is_empty()
        );
    }

    #[test]
    fn subscription_message_shape() {
        let mut codec = PolymarketCodec {
            verify_top_of_book: true,
        };
        let messages = codec.subscribe(&["1".into(), "2".into()]);
        let value: serde_json::Value = serde_json::from_str(&messages[0]).unwrap();
        assert_eq!(value["type"], "market");
        assert_eq!(value["operation"], "subscribe");
        assert_eq!(value["assets_ids"], json!(["1", "2"]));
    }

    #[test]
    fn gamma_market_filtering() {
        let market: GammaMarket = serde_json::from_str(
            r#"{"clobTokenIds":"[\"1\",\"2\"]","enableOrderBook":true,"acceptingOrders":true,"active":true,"closed":false}"#,
        )
        .unwrap();
        assert_eq!(market.live_tokens(), vec!["1", "2"]);
        let closed: GammaMarket = serde_json::from_str(
            r#"{"clobTokenIds":"[\"1\"]","enableOrderBook":true,"closed":true}"#,
        )
        .unwrap();
        assert!(closed.live_tokens().is_empty());
    }
}
