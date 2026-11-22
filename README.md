# java-feed-handler

A modular JVM feed handler for Nasdaq TotalView-ITCH over MoldUDP64. The
decoded `MarketMessage` contract and two interchangeable single-instrument
order books are implemented. UDP ingress, ITCH decoding, application wiring,
and benchmarks are not implemented yet.

## Build

On macOS or Linux, install JDK 26 and set `JAVA_HOME` to that installation.
The checked-in Gradle Wrapper pins Gradle 9.9.2. In IntelliJ IDEA, use the
Gradle Wrapper for the Gradle distribution and JDK 26 for the Gradle JVM.

```sh
./gradlew build
./gradlew :order-book:test
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

See [ADR-004](docs/adr/ADR-004-build-toolchain-monorepo.md) for the toolchain and module boundaries.