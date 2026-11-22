# java-feed-handler

A modular JVM feed handler for Nasdaq TotalView-ITCH over MoldUDP64. The
repository currently contains the build scaffold; feed processing and
benchmarks are not implemented yet.

## Build

On macOS or Linux, install JDK 25 and set `JAVA_HOME` to that installation.
The checked-in Gradle Wrapper supplies Gradle.

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

See [ADR-004](docs/adr/ADR-004-jdk25-gradle-monorepo.md) for the module boundaries.