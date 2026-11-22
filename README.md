# java-feed-handler

A modular JVM feed handler for Nasdaq TotalView-ITCH over MoldUDP64. The
decoded `MarketMessage` contract, two interchangeable single-instrument
order books, and an in-memory MoldUDP64 A/B arbiter are implemented. Live
UDP ingress, ITCH decoding, application wiring, and benchmarks are not
implemented yet.

## Build

On macOS or Linux, install JDK 26 and set `JAVA_HOME` to that installation.
The checked-in Gradle Wrapper pins Gradle 9.9.2. In IntelliJ IDEA, use the
Gradle Wrapper for the Gradle distribution and JDK 26 for the Gradle JVM.

```sh
./gradlew build
./gradlew :order-book:test
./gradlew :nasdaq-feed:test
```

| Module | Responsibility |
|---|---|
| `:feed-core` | Protocol-independent event, snapshot, and handoff contracts |
| `:order-book` | Single-writer book implementations |
| `:nasdaq-feed` | UDP ingress, MoldUDP64 arbitration, and ITCH mapping |
| `:app` | Application wiring and loopback demo |
| `:benchmarks` | JMH benchmarks, run separately with `./gradlew :benchmarks:jmh` once added |

Both books apply add, execute, and delete events through `OrderBook`. The
reference uses `HashMap` and `TreeMap`; the packed version uses preallocated
order slots and sorted primitive arrays per side, with the best price at index
zero. See [ADR-001](docs/adr/ADR-001-reference-order-book.md) for the contract
and layout tradeoffs.

The arbiter accepts datagram bytes from either leg and emits one ordered
stream of raw ITCH messages. Its tests use synthetic datagrams and a
controllable clock; no sockets or capture files are required. Packet
descriptors and reorder storage can be heap-backed or pooled. A synchronous
sink reads in-order payloads directly from the caller's buffer; only delayed
payloads are copied into reorder storage. The collecting API still returns
independently owned copies. See
[ADR-002](docs/adr/ADR-002-portable-udp-feed.md#accepted-first-slice-in-memory-arbitration).

See [ADR-004](docs/adr/ADR-004-build-toolchain-monorepo.md) for the toolchain and module boundaries.