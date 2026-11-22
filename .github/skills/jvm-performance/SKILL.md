---
name: jvm-performance
description: Design and interpret reproducible JVM performance experiments using JMH, JFR, and CPU hardware counters. Use when benchmarking allocation, queue handoffs, order-book layouts, tail latency, or cache misses.
---

# JVM performance and microbenchmarking

1. Define what the benchmark times and verify equivalent behavior before
   comparing implementations. Change one variable at a time: keep the book
   fixed when comparing handoffs, and keep the handoff fixed when comparing
   books. Use the same event mix and parameterize working-set size and access
   pattern instead of assuming the benchmark represents live traffic.
2. Use JMH for isolated operations. Warm up each fork, use multiple forks,
   consume results to prevent dead-code elimination, and keep fixture setup
   out of the measured path unless it is part of the cost. Use `-prof gc` to
   measure allocation per operation and GC activity. When comparing `new`
   with slots, distinguish local objects HotSpot may scalar-replace from
   objects actually transferred across threads.
3. Use a paced workload for cross-thread or end-to-end tail latency. Measure
   per event, including the wait in the queue, not percentiles of batch
   averages. Drive arrivals independently of consumer progress to avoid
   coordinated omission. Report p50, p99, and p99.9 with sample counts,
   achieved throughput, queue-full events, and drops at the same offered
   load. Label the timestamp boundaries; do not present a replay or compute
   benchmark as NIC-to-book latency.

## Check allocation-free paths with Epsilon GC

For paths intended not to allocate after warmup, run a separate sustained
workload with `-XX:+UnlockExperimentalVMOptions -XX:+UseEpsilonGC` and an
explicit fixed heap (`-Xms` equal to `-Xmx`). Epsilon never reclaims objects,
so recurring heap allocation accumulates even when the objects are short-lived.

Report the heap size, warmup, duration or event count, and heap usage over
the steady-state run. Require correct output, no `OutOfMemoryError`, and no
sustained growth in used heap after warmup. Completing a finite run alone
only shows that its allocations fit in the heap. This check covers Java
heap usage, not off-heap memory; track direct or native buffers separately
if claiming a process-wide bound. Use the normal collector for headline
performance comparisons and workloads intended to allocate.

## Diagnose in separate profiling runs

- **JFR:** Repeat the same workload with relevant GC, allocation, JIT
  compilation, and deoptimization events enabled. Compare their timing
  against slow periods before attributing a latency change to GC or JIT.
- **Hardware counters:** On Apple silicon, use
  [Instruments CPU Counters](https://developer.apple.com/documentation/xcode/addressing-cpu-bottlenecks).
  On Linux, use `perf list` to discover events supported by the host CPU and
  `perf stat` to measure them. Name the event and cache level, and normalize
  counts by processed events; report a miss rate only with a matching access
  count. Check counter scope and multiplexing. Do not equate similarly named
  counters across CPU architectures or infer cache misses from latency alone.

Keep profiling runs separate from headline timings. If the required counter
is unavailable, state that the cache explanation remains a hypothesis.

## Keep an official benchmark history

Save a version-controlled report for each benchmark run used to establish a
baseline or support a performance claim. Keep reports under
`docs/benchmarks/<benchmark>/<date>-<commit>-<run-id>.md`. Add a new report
instead of overwriting an earlier result.

- Record the timestamp, exact commit and any local changes, reproduction
  command, workload or fixture version, and what the measurement includes.
  Include the JDK, JVM flags and GC, OS, CPU, and relevant thread placement.
- Record results with units and sample counts. Preserve or link the raw JMH
  JSON, latency histograms, or counter output used in the report. Label
  JFR, counter, and Epsilon runs as diagnostics, not headline timings.
- Link the prior baseline and report changes only when the workload,
  measurement boundaries, and environment are comparable. If they differ,
  say what changed rather than presenting the delta as a regression or gain.
