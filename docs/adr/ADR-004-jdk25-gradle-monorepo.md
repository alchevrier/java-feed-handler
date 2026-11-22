# ADR-004: Use JDK 25 and Gradle in a modular monorepo

**Status: Accepted**

## Context

The feed handler should build and run on macOS and Linux. The same repository
should hold the reusable contracts, the Nasdaq-specific pipeline, and the
benchmarks so a change can update all three without coordinating releases.

## Proposed decision

**JDK 25.** Use JDK 25 as the long-term support baseline for compilation,
tests, application runs, and benchmarks. HotSpot's
[compact object headers][headers] became a non-experimental, opt-in feature
in JDK 25 (`-XX:+UseCompactObjectHeaders`). Smaller headers may reduce the
footprint of heap objects in the reference implementation. Keep the default
layout for the initial baseline and measure compact headers separately
before making them a project default. JDK 25 is a supported baseline,
not a claim that it is the fastest runtime for this workload.

[Virtual threads][virtual] have been available since JDK 21 and are an option
for future blocking workloads. Keep dedicated platform threads for the
receivers, arbiter, and book writer in [ADR-002](ADR-002-portable-udp-feed.md).
Virtual threads help scale blocking workloads; they do not by themselves
make the hot path faster.

**Gradle.** Pin the Gradle Wrapper to 9.3.0 and use a shared Java 25
toolchain. The [compatibility matrix][compatibility] lists 9.1.0 as the
first release that supports running on JDK 25. The wrapper makes builds
consistent across machines, while
incremental execution and build caching can shorten repeated builds.
Use Spock for correctness tests and keep JMH out of the application
runtime dependencies.

**Monorepo.** Use one [Gradle multi-project build][multi-project], with
subprojects grouped by responsibility rather than one per interface:

| Subproject | Responsibility | Project dependencies |
|---|---|---|
| `:feed-core` | Shared book-event and snapshot contracts, and the bounded handoff contract and reference adapter. No wire-protocol or socket dependencies. | None |
| `:order-book` | Single-writer book implementations and the shared book correctness specs. | `:feed-core` |
| `:nasdaq-feed` | UDP adapters, MoldUDP64 framing and A/B arbitration, and the TotalView-ITCH mapper. | `:feed-core` |
| `:app` | Select and wire implementations at startup; run the portable loopback demo. | `:feed-core`, `:order-book`, `:nasdaq-feed` |
| `:benchmarks` | Run JMH and workload comparisons against the production components. | Components or application needed by the workload; never a dependency of production code. |

Keep project dependencies acyclic. In particular, the book must not depend
on the Nasdaq transport or decoder; they meet at the decoded event contract.
Scaffold all five subprojects up front to establish the dependency boundaries.
Add source code and feature tests only when the corresponding ADR is approved.

## Alternatives and tradeoffs

- **JDK 21:** Also an LTS and supports virtual threads, but lacks compact
  object headers in upstream HotSpot. This project has no older-JDK
  compatibility requirement.
- **Maven:** Also supports wrappers and multi-module builds. Gradle is the
  proposed choice for its incremental and cacheable task model and its
  ability to run individual subproject tasks.
- **One module or separate repositories:** One module has less build
  configuration but does not enforce these dependency boundaries.
  Separate repositories make independent releases possible but add
  coordination costs for changes that span contracts and implementations.

## Acceptance criteria

From the repository root, `./gradlew build` on JDK 25 compiles all implemented
subprojects and runs the full correctness suite. A developer can also run
an individual subproject's tests. Benchmarks run explicitly, not as part of
the correctness gate. Benchmark reports record the actual JDK build and
JVM flags, including whether compact headers were enabled, as required by
[ADR-003](ADR-003-performance-baselines.md).

[headers]: https://openjdk.org/jeps/519
[virtual]: https://openjdk.org/jeps/444
[compatibility]: https://docs.gradle.org/current/userguide/compatibility.html#java_runtime
[multi-project]: https://docs.gradle.org/current/userguide/multi_project_builds.html
