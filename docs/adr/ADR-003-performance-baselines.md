# ADR-003: Verify the composed pipeline before benchmarking

**Status: Accepted for the first paced loopback baseline and book layout
comparison.** Component microbenchmarks and profiling remain proposed.

**Depends on:** [ADR-001](ADR-001-reference-order-book.md) and
[ADR-002](ADR-002-portable-udp-feed.md) for the correct, runnable reference
pipeline.

## Context

The project should show what each optimization buys, not just publish the
fastest result from an unrepeatable run.

## Proposed decision

1. **Compose and verify the reference pipeline.** Connect both UDP
   listeners, A/B arbitration, the ITCH mapper, the bounded handoffs, and
   the reference book through snapshot publication. Pass the component
   specs and a synthetic loopback check of the composed path, including
   ordered application, duplicate suppression, and visible stale status.
   Get a green JDK 26 build before starting performance work.
2. **Measure the composed baseline.** Record the full UDP-to-snapshot
   reference run under steady and bursty load, with the reference
   implementations unchanged.
3. **Isolate and verify changes.** Benchmark individual components and
   connected stages, such as mapper-to-book handoff, to explain the
   baseline and test one change at a time. Run the same correctness
   contract against each variant. Put the change back into the full pipeline
   and rerun the same workload to see whether the improvement carries over.

The first comparisons are:

1. **Mapper handoff:** Hold the book fixed. Compare bounded
   `LinkedBlockingQueue` with fresh DTOs, an SPSC ring with fresh DTOs, and an
   SPSC ring with direct mapping into preallocated slots. Keep offered load,
   capacity, and batch size constant.
2. **Book layout:** Hold the handoff fixed. Compare `HashMap` + `TreeMap` with
   packed sorted primitive price levels and an array-backed order index. Vary active
   order count and include both quantity-only updates and removals that
   empty the best level, where the packed layout may shift entries.

Use JMH with multiple forks and warmup for isolated operations; measure
allocation with `-prof gc`. For paced handoff and UDP-to-snapshot tests,
measure individual events, including queue wait within the stated timing
boundary. Report p50, p99, and p99.9 alongside sample count, offered and
achieved rates, queue-full events, and drops. Do not report loopback or
in-memory replay as NIC-to-book latency.

In diagnostic runs, correlate stage measurements by session and message
sequence where practical. Separate processing time from time waiting in a
queue or for a missing sequence. Do not add component percentiles to
estimate end-to-end latency; use the measured end-to-end distribution.

Profile separately from headline timings. Use JFR for allocation, GC, and
JIT evidence; inspect generated code when testing a JIT explanation. Use
thread-state traces to distinguish on-CPU work, blocked waits, and runnable
scheduling delays. Use Instruments on supported macOS hardware and `perf`
on Linux; claim a cache effect only when the relevant counters support it.
Use a separate fixed-heap Epsilon GC run to probe steady-state heap growth
on paths intended to be allocation-free; surviving a finite run does not
prove a global memory bound.

### Accepted first slice: paced UDP-to-publication baseline

Keep the current ingress, arbiter, mapper, and bounded ring; use the
`HashMap`/`TreeMap` book. Send the same sequence of synthetic add, execute,
and delete updates on both loopback legs. Compare even spacing and batches
of 16 at the same mean offered rate. Warm up each workload on fresh state
before recording it. The sender follows absolute deadlines independently
of consumer progress and does not retry missing data messages. A final
heartbeat advertises the end of the sequence range.

Record each successful snapshot publication by sequence, not by polling
the latest snapshot. Measure from both the scheduled deadline and the
first socket send attempt. The former includes sender lateness; neither
includes a strategy read or measures physical NIC latency. Record the
sample count and unobserved updates alongside percentiles; do not treat
an incomplete or stale run as a lossless latency result. Save per-event
samples as CSV and report per-leg application drops and the terminal
reason, including ring saturation. This slice does not compare the book
layouts, tune affinity, or change the publishing contract.

### Accepted comparison: packed and reference books on the same trace

Run both book implementations with the same deadlines, ingress, capacity,
park policy, and publication code. Warm up each case and alternate which
book goes first between steady and bursty. This checks whether a book
layout difference is visible in the composed loopback pipeline. The
one-active-order trace does not test book depth, and the snapshot path
uses each book's own quote implementation, so this is not an isolated
comparison of mutation costs.

## Alternatives and tradeoffs

An end-to-end benchmark alone is representative of the combined path but
cannot isolate the cause of an improvement. Microbenchmarks alone isolate
cost but do not capture queueing under load. Use both and label the boundary
of each. Synthetic input is sufficient for the first baseline; in-memory
and UDP PCAP replay can later test the same captured payloads at different
ingress boundaries. Keep those results separate from raw ITCH replay and
from the initial no-audit baseline.

## Acceptance criteria

Preserve the first reference runs and later comparisons as versioned reports
under `docs/benchmarks/`. Each report identifies the commit, workload,
environment, command, results, and available raw artifacts. Link the prior
baseline and report deltas only for comparable runs. Explain whether each
component-level improvement carried over to the full pipeline and
distinguish measured causes from hypotheses. There is no fixed nanosecond
target for this milestone.

Sources: [Benchmark draft](../draft/ingress-comparison.md#benchmark-sketch)
and the [JVM performance skill](../../.github/skills/jvm-performance/SKILL.md).
