# ADR-004: Use JDK 26 and Gradle in a modular monorepo

**Status: Accepted**

## Context

The feed handler should build and run on macOS and Linux. The same repository
should hold the reusable contracts, the Nasdaq-specific pipeline, and the
benchmarks so a change can update all three without coordinating releases.

## Decision

**JDK 26.** Use JDK 26 for compilation, tests, application runs, and
benchmarks, aligning the build toolchain with the development and IDE
runtime. HotSpot's
[compact object headers][headers] became a non-experimental, opt-in feature
in JDK 25 (`-XX:+UseCompactObjectHeaders`). Smaller headers may reduce the
footprint of heap objects in the reference implementation. Keep the default
layout for the initial baseline and measure compact headers separately
before making them a project default. This runtime choice does not imply a
performance gain.

[Virtual threads][virtual] have been available since JDK 21 and are an option
for future blocking workloads. Keep dedicated platform threads for the
receivers, arbiter, and book writer in [ADR-002](ADR-002-portable-udp-feed.md).
Virtual threads help scale blocking workloads; they do not by themselves
make the hot path faster.

**Gradle.** Pin the Gradle Wrapper to 9.9.2, the current stable release at
the time of the upgrade. It [supports running on JDK 26][compatibility],
unlike the original Gradle 9.3.0 wrapper. Use the wrapper in the IDE as well
as on the command line. Incremental execution and build caching can
shorten repeated builds.
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

- **JDK 25:** The original LTS baseline also supports compact headers and
  virtual threads. JDK 26 aligns with the installed development runtime
  but is a non-LTS release, so the project will need to revisit the
  toolchain sooner.
- **Maven:** Also supports wrappers and multi-module builds. Gradle is the
  proposed choice for its incremental and cacheable task model and its
  ability to run individual subproject tasks.
- **One module or separate repositories:** One module has less build
  configuration but does not enforce these dependency boundaries.
  Separate repositories make independent releases possible but add
  coordination costs for changes that span contracts and implementations.

## Acceptance criteria

From the repository root, `./gradlew build` on JDK 26 compiles all implemented
subprojects and runs the full correctness suite. A developer can also run
an individual subproject's tests. Benchmarks run explicitly, not as part of
the correctness gate. Benchmark reports record the actual JDK build and
JVM flags, including whether compact headers were enabled, as required by
[ADR-003](ADR-003-performance-baselines.md).

[headers]: https://openjdk.org/jeps/519
[virtual]: https://openjdk.org/jeps/444
[compatibility]: https://docs.gradle.org/current/userguide/compatibility.html#java_runtime
[multi-project]: https://docs.gradle.org/current/userguide/multi_project_builds.html
