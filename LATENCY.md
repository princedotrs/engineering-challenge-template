# Latency experiments (Extension 4)

Data correctness and completeness come first. Latency only decides *which*
healthy connection the collector trusts for a symbol. This file covers what is
built in, the tool for running experiments, and the experiments themselves.

> **Status:** no live measurements yet. The environment this was built in had
> no network route to Polymarket or Kalshi, so the results tables below are
> blank. Fill them in by running the commands below.

## What is built into the collector

* Every frame is timestamped as soon as it comes off the socket, before
  decoding (`received_at`).
* Each connection keeps a moving average of `received_at - exchange_timestamp`.
  The clock offset between us and the exchange is the same on every connection,
  so it cancels out when connections are compared.
* For each symbol the engine prefers the connection that is clearly faster
  (by more than 50 ms), switching at most once per 30 s. A switch writes a
  snapshot, so correctness doesn't depend on the two books being identical.
* Nagle's algorithm is disabled (`TCP_NODELAY`) on every websocket, so small
  frames such as pings and subscriptions aren't delayed. Encoding and disk
  writes run on a dedicated thread, away from socket reads.

## The probe

```bash
# 6 parallel connections, same 50 symbols, 5 minutes
engineering-challenge probe polymarket --connections 6 --symbols 50 --duration-secs 300

# Same, but pin each connection's local port to 40000+i
engineering-challenge probe polymarket --connections 6 --source-port-base 40000

# Kalshi (needs KALSHI_API_KEY_ID / KALSHI_PRIVATE_KEY_PATH)
engineering-challenge probe kalshi --connections 6 --symbols 50
```

For each connection it prints:

| column | meaning |
|---|---|
| `local` / `remote` | Socket addresses. Different remote IPs mean different load-balancer or edge nodes. |
| `handshake` | DNS + TCP + TLS + websocket upgrade time |
| `p50/p99 lat` | `received_at - exchange_timestamp`, which includes our clock offset |
| `wins` | Share of updates this connection delivered **first** among all connections |
| `p50/p99 behind` | How far behind the fastest connection this one delivered the same update |

`wins` and `behind` are measured on our own clock only, so they are the fairest
way to compare connections.

## Experiments to run

1. **Baseline spread.** Six connections, the same symbols, five minutes. Is
   one connection consistently ahead, or does the winner change at random? A
   stable spread means choosing connections is worth doing. Random winners mean
   redundancy helps tail latency but picking connections doesn't.
2. **Spam and keep the fastest.** Open many connections (say 20), keep the
   best few by `p50 behind`, and re-run to see whether the ranking holds. If it
   does, the collector could open extra connections at startup and keep the
   fastest ones.
3. **Source port.** Run with `--source-port-base` across several bases. If
   the remote IP or the `behind` numbers change with the local port, the
   exchange's load balancer is hashing on the connection 4-tuple, and port
   choice is a cheap way to reach a better server.
4. **DNS / edge.** Note the `remote` IPs. If the hostname resolves to several
   addresses, compare them by pinning (for example with `/etc/hosts`) and
   re-running.
5. **After maintenance.** Kalshi has scheduled maintenance windows. Run the
   probe just after the exchange comes back, when the first connections may
   land on the least-loaded servers, and compare with a run mid-session.
6. **Load per connection.** Compare `--symbols 10` with `--symbols 500` on the
   same number of connections. If `behind` grows with symbols per connection,
   the exchange is buffering per connection, and `--symbols-per-connection`
   should be lowered.
7. **Geography.** The biggest gain is usually where the collector runs. Repeat
   from a few cloud regions near each exchange's hosting.

## Results

| Date | Exchange | Experiment | Setup | Finding |
|---|---|---|---|---|
| | | | | |
