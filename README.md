# Order book collector

A service that connects to Kalshi or Polymarket, subscribes to every live
market, keeps an order book per symbol, and writes **every price level change
plus a full snapshot of every book each minute** using the SBE helpers in this
repo. New markets are picked up and closed ones dropped while it runs. On
`SIGINT` it closes its websockets (it sends a close, then keeps reading for up
to 1 s so frames already in flight are kept), writes out everything it
received, and closes the output file. If the output can't be written (disk
full, closed pipe), it shuts down with an error instead of losing data
silently.

All four extensions are implemented: redundant websockets with failover,
subscription monitoring and connection recycling, an exchange-agnostic core
with a small per-exchange codec, and a latency probe (see
[LATENCY.md](LATENCY.md)).

## Quick start

```bash
cargo build --release

# Polymarket: no account needed. Data goes to stdout, logs to stderr.
./target/release/engineering-challenge polymarket > polymarket.sbe

# Or rotate into timestamped files, one per hour
./target/release/engineering-challenge polymarket --output-dir data/ --rotate-minutes 60

# Kalshi: needs an API key (Kalshi → Account → API keys)
export KALSHI_API_KEY_ID=...            # the key id
export KALSHI_PRIVATE_KEY_PATH=key.pem  # the RSA private key Kalshi gave you
./target/release/engineering-challenge kalshi --output kalshi.sbe

# Try it on a few markets first
./target/release/engineering-challenge polymarket --max-symbols 20 -o sample.sbe

# Read a file back (uses AlignedSbeReader) and sanity-check it by replaying the books
./target/release/engineering-challenge dump sample.sbe | head
./target/release/engineering-challenge dump --quiet sample.sbe
```

Press `Ctrl-C` once for a clean shutdown (a second `Ctrl-C` forces exit).
Logging is controlled with `RUST_LOG` (for example `RUST_LOG=debug`). Every
tunable is listed under `--help` on each subcommand.

> The writer pads output into 64 MiB chunks (that is how `AlignedSbeWriter`
> works), so data on stdout appears in 64 MiB blocks, and the final partial
> chunk is written on shutdown or rotation. On Windows, use `--output` or
> `--output-dir` rather than stdout.

## Output

Two message types from `sbe_schemas/engineering_challenge/challenge_orderbook.yaml`:

* **`Snapshot`**: the full book (bids best-first, asks best-first). Written
  when a symbol is first synced, once per minute for every symbol (on wall-clock
  minute boundaries), at the start of every rotated file, and whenever the
  collector switches which websocket a symbol is sourced from.
* **`PriceLevelUpdate`**: the new *absolute* quantity at one price level
  (`0` = level removed).

Prices and quantities are fixed point, `1_000_000 = 1`. Each message has
two timestamps:

* `received_at` / `backtest_timestamp`: when the frame came off the socket
  (taken before decoding).
* `exchange_timestamp`: the exchange's own timestamp, when it sends one.

**Schema change.** `PriceLevelUpdate` had no side, so a reader could not tell
whether an update at, say, 0.50 belongs to the bids or the asks. I added a
`side` field (`BookSide`: `Bid` / `Ask`) and bumped the schema version to 2.
That is the only change to the provided files.

**Kalshi books** only contain YES bids and NO bids. A NO bid at `p` is a YES ask
at `1 - p`, so Kalshi books are written from the YES side: YES bids are bids,
NO bids become asks at `1 - p`. **Polymarket** symbols are CLOB token ids. Each
outcome token has its own book.

## Architecture

```
            ┌─────────────┐   live symbols    ┌───────────────────────────────┐
REST  ────▶ │ Supervisor  │ ────────────────▶ │ shard 0: symbols 0..200       │
(discovery) │ diff + shard│                   │   conn 0 ─┐  conn 1 ─┐        │ websockets
            └─────────────┘                   │ shard 1 ...         ...       │ ◀────────▶ exchange
                                              └───────────┼─────────┼─────────┘
                                                normalised│events   │
                                                          ▼         ▼
                         ┌─────────────────────────────────────────────────┐
                         │ Engine: one book per symbol per connection,     │
                         │ picks a source connection per symbol, health    │
                         │ checks, failover snapshots, 1-minute snapshots   │
                         └──────────────────────┬──────────────────────────┘
                                                ▼  (channel)
                                      writer thread → AlignedSbeWriter → file / stdout
```

| Module | Role |
|---|---|
| `exchange/mod.rs` | The `Exchange` + `Codec` traits: the only exchange-specific surface |
| `exchange/polymarket.rs`, `exchange/kalshi.rs` | Discovery, auth, subscribe/unsubscribe messages, decoding into normalised `Event`s |
| `supervisor.rs` | Periodic discovery, diffing, assigning symbols to shards, spawning connections |
| `connection.rs` | One websocket: connect with backoff, subscribe, keepalive, idle detection, recycling |
| `engine.rs` | Books, source selection, failover, subscription health, periodic snapshots |
| `output.rs` | Writer thread, file rotation, clean close |
| `book.rs`, `fixed.rs` | Order book; exact decimal-string → fixed-point parsing (no floats) |
| `dump.rs`, `probe.rs` | Reading output back; latency experiments |

The core is generic over `Exchange`, so adding an exchange means
implementing `discover`, `connect_request` and a `Codec`
(`subscribe` / `unsubscribe` / `decode` / `keepalive`). The end-to-end test
plugs in a mock exchange this way.

### Load balancing

Symbols are split into shards of `--symbols-per-connection` (default 200 for
Polymarket, 250 for Kalshi). New symbols fill gaps left by removed ones before
a new shard is created. New connections, including reconnects, go through a
global rate limiter (`--connects-per-second`) so a mass reconnect doesn't trip
exchange limits.

### Completeness checks

* **Kalshi:** each ticker is its own subscription, so a gap in `seq` points to
  exactly one book. That book is marked bad and resubscribed for a fresh snapshot.
* **Polymarket:** there are no sequence numbers, but every `price_change` carries
  the exchange's best bid/ask after the change. Each book is checked against it
  and resubscribed on a mismatch (`--no-verify-top-of-book` turns this off).
* **Both exchanges:** a negative quantity after a delta, or a book that stays
  crossed for more than 5 s, is treated the same way.
* **Discovery:** a symbol is only dropped after it is missing from two
  complete listings in a row, and a listing that fails partway can add
  symbols but never removes them. This means a flaky REST page can't
  unsubscribe live markets.

## Extensions

### 1. Redundant websockets

Every shard is carried by `--replicas` (default 2) independent connections.
Each connection builds its **own** copy of every book from only what it
received. For each symbol the engine picks one connection as the *source*;
only the source's changes are written. The engine switches to another
connection when the source:

* disconnects or stops sending frames (idle timeout), which covers
  disconnects with no warning and crashed sockets;
* reports a sequence gap, fails a top-of-book check, or stays crossed;
* goes silent for a symbol while the other connection keeps getting updates
  for it (a subscription that quietly stopped);
* is consistently slower: each connection's latency (receive time minus
  exchange timestamp) is tracked, and the engine switches when another
  connection is faster by more than 50 ms. It switches at most once per 30 s
  per symbol to avoid flapping.

A connection whose updates for a symbol are well behind its sibling's is
never chosen as the new source. If no connection is usable, the symbol is
picked up again as soon as one becomes usable.

On every switch the engine writes a **snapshot from the new source's book**, as
the challenge asks. When the current source receives a fresh snapshot from the
exchange (for example after a resubscribe), the difference is written as level
updates, so the output stays a continuous stream of deltas.

### 2. Subscription monitoring

* **Silent subscription:** if one connection hasn't seen an update for a
  symbol in 60 s while the other connection has, that subscription is
  unsubscribed and resubscribed.
* **Quiet symbol:** the engine tracks each symbol's usual gap between updates.
  If *every* connection has been silent for 20× that gap (clamped to 5–60 min),
  one connection at a time is resubscribed, never the current source while
  another exists, so coverage holds while it checks the market is really idle.
* **No snapshot after subscribing:** the subscription is retried after 30 s.
* Resubscribes of the same symbol on the same connection are rate-limited
  (10 s).
* **Recycling:** every connection is fully disconnected and reconnected every
  `--recycle-minutes` (default 30). Each replica in a shard has a fixed,
  evenly spaced slot in that schedule, and shards get a random offset so they
  don't all recycle at once. A connection never recycles while a sibling
  replica is down; it waits until the sibling is back.
* **Reconnect backoff** (1 s doubling to 60 s, with jitter) only resets once a
  connection has stayed up for 30 s. A server that accepts and immediately
  drops us isn't retried every second.

### 3. Exchange-agnostic core

See [Architecture](#architecture). Kalshi and Polymarket differ only in their
`Exchange` and `Codec` implementations. The codecs emit a small normalised
`Event` set: `Snapshot`, `Delta` (an absolute `Set` or a relative `Add`),
`TopOfBook` and `Desync`.

### 4. Latency

`engineering-challenge probe <exchange>` opens several connections to the same
symbols and reports handshake time, the remote IP each connection landed on,
receive latency, and how often each connection is first to deliver an update.
It can also pin the local source port. See [LATENCY.md](LATENCY.md) for the
approach and the experiments to run.

## Testing

```bash
cargo test
```

* Unit tests: fixed-point parsing, the book, both codecs (snapshots, deltas,
  Kalshi sequence gaps, YES/NO mapping, late subscription confirmations, request
  signing), and the engine policies (source-only writes, failover snapshot,
  desync, silent replica, latency switch, periodic snapshots).
* `tests/mock_exchange.rs`: an end-to-end test against a local websocket server
  speaking the Polymarket protocol. It drops a connection without warning,
  silently mutes another, and removes a symbol mid-run, then checks that
  replaying the collector's output reproduces the server's books exactly.
* `tests/template_roundtrip.rs`: the template's original `main.rs` example.

## Limitations and notes

* **Not yet run against the live exchanges.** The development environment
  couldn't reach Polymarket or Kalshi. Message formats were taken from
  Polymarket's official Rust client (`rs-clob-client`) and Kalshi's official
  starter code and Python SDK models. A short live run with `--max-symbols` is
  the first thing to do. Kalshi parsing accepts both the current
  `*_dollars_fp` string fields and the older integer-cent fields.
* Per-connection symbol limits and connection-rate limits aren't fully
  documented by either exchange, so they are flags rather than constants.
* Polymarket's `hash` field is not verified. The best bid/ask check covers most
  of the same failures.
* All engine work runs on one task. That is plenty for these exchanges' message
  rates, but the queue length is logged with the periodic stats in case it ever
  becomes a bottleneck.

---

## About the template

This repo uses the [FIX SBE standard](https://fixtrading.org/packages/simple-binary-encoding-technical-specification-v2-rc2/)
for writing binary messages. The rust code abstracts this away, and the helpers
in `src/util` are used to write and read every message.

FIX SBE is defined in XML, which is cumbersome to edit when updating schemas
frequently, so this repo has helper code that takes a custom YAML format and
turns it into FIX SBE XML, which in turn is used to generate rust code to encode
and decode the messages. The YAML schema lives in
[challenge_orderbook.yaml](sbe_schemas/engineering_challenge/challenge_orderbook.yaml).
A custom timestamp field lets the Rust struct use `chrono::DateTime`; in the SBE
schema it is a uint64 of microseconds since the unix epoch.
