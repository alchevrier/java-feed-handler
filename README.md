# java-feed-handler

A modular JVM feed handler for Nasdaq TotalView-ITCH over MoldUDP64. The
decoded `MarketMessage` contract, two interchangeable single-instrument
order books, an in-memory MoldUDP64 A/B arbiter, an ITCH add/execute/delete
mapper, and a bounded mapper-to-book handoff are implemented. Live UDP
ingress, application wiring, and benchmarks are not implemented yet.

## Build

On macOS or Linux, install JDK 26 and set `JAVA_HOME` to that installation.
The checked-in Gradle Wrapper pins Gradle 9.9.2. In IntelliJ IDEA, use the
Gradle Wrapper for the Gradle distribution and JDK 26 for the Gradle JVM.

```sh
./gradlew build
./gradlew :order-book:test
./gradlew :nasdaq-feed:test
```

| Module | Current scope |
|---|---|
| [`:feed-core`](feed-core/README.md) | Protocol-independent order-event contract |
| [`:order-book`](order-book/README.md) | Reference and packed single-writer books |
| [`:nasdaq-feed`](nasdaq-feed/README.md) | In-memory MoldUDP64 framing, A/B arbitration, ITCH mapping, and book handoff |
| [`:app`](app/README.md) | In-memory integration tests; scaffold for application wiring and the loopback demo |
| [`:benchmarks`](benchmarks/README.md) | JMH scaffold; workloads not yet implemented |

Both books apply add, execute, and delete events through `OrderBook`. The
reference uses `HashMap` and `TreeMap`; the packed version uses preallocated
order slots and sorted primitive arrays per side, with the best price at index
zero. See [ADR-001](docs/adr/ADR-001-reference-order-book.md) for the contract
and layout tradeoffs.

The arbiter accepts datagram bytes from either leg and emits one ordered
stream of raw ITCH messages. Its tests use synthetic datagrams and a
controllable clock; no sockets or capture files are required. Packet
descriptors and reorder storage can be heap-backed or pooled. With
`acceptAndForward`, a synchronous sink reads in-order payloads directly
from the caller's buffer; only delayed payloads are copied into reorder
storage. The ITCH mapper reads these ranges directly and sends decoded
fields to a heap-backed or reusable synchronous target, or directly into
preallocated slots for a separate book thread. Call `acceptAndCopy`
explicitly when you need independently owned copies of raw payloads. See
[ADR-002](docs/adr/ADR-002-portable-udp-feed.md#accepted-first-slice-in-memory-arbitration).

See [ADR-004](docs/adr/ADR-004-build-toolchain-monorepo.md) for the toolchain and module boundaries.