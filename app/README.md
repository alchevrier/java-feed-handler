# `:app`

**Status: in-memory integration tests only.** The tests connect the arbiter
and ITCH mapper to both order books using synthetic datagrams. Run them with
`./gradlew :app:test` from the repository root. There is no application entry
point or runnable loopback demo yet.

## Planned loopback flow

```mermaid
flowchart LR
    A["Synthetic UDP leg A"] --> RXA["UDP receiver A"]
    B["Synthetic UDP leg B"] --> RXB["UDP receiver B"]
    RXA --> QA["Bounded ingress queue A"]
    RXB --> QB["Bounded ingress queue B"]
    QA --> Arbiter["A/B arbiter"]
    QB --> Arbiter
    Arbiter --> Mapper["ITCH mapper"]
    Mapper --> Events["Bounded book-event queue"]
    Events --> Book["Single-writer OrderBook"]
    Book --> Sink["Snapshot and status publisher"]
```

The planned demo will exercise the portable UDP-to-snapshot path with synthetic
datagrams. The app will compose the arbiter and ITCH mapper from
[`nasdaq-feed`](../nasdaq-feed/README.md), the event contract from
[`feed-core`](../feed-core/README.md), and either
[`order-book`](../order-book/README.md) implementation. The arbiter and mapper
will share a thread; the book will own its state on a separate thread.

See [ADR-002](../docs/adr/ADR-002-portable-udp-feed.md) for the proposed topology
and backpressure policy.
