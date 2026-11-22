# Comparing MoldUDP64 ingress paths

**Status: Draft.** These are candidates, not an accepted ADR.

Compare three ways of receiving the same UDP datagrams while keeping MoldUDP64
sequencing, ITCH decoding, book updates, and publication unchanged.

| Candidate | Receive path | Key constraint |
|---|---|---|
| Java `DatagramChannel` | The normal OS UDP stack delivers the datagram payload to Java. | Portable macOS/Linux baseline; reuse direct receive buffers. |
| XDP + AF_XDP | An XDP eBPF program redirects frames early in the Linux receive path to an AF_XDP socket. | AF_XDP is the receive interface; zero-copy mode depends on NIC and driver support. Record whether the run uses copy or zero-copy mode. |
| DPDK | A userspace hardware poll-mode driver receives frames directly from the NIC, outside the normal kernel network stack. | Requires compatible hardware and a native boundary to the JVM. Include that boundary in the measurement. |

`DatagramChannel` hands the adapter a UDP payload. AF_XDP and DPDK hand it
frames, so those adapters need to extract the UDP payload before handing it to
the shared MoldUDP64 parser. The native integration approach is still open.

## Replay and correctness inputs

Use the same extracted MoldUDP64 payloads in two PCAP replay modes. Preserve
the A/B leg identity and, for paced runs, the relative arrival times across
both legs.

| Input | Where it enters | What it exercises |
|---|---|---|
| In-memory PCAP replay | Extract and preload UDP payloads, then feed them to the arbiter with their leg identity. | Arbitration, ITCH decoding, and book updates without the socket receive path. Exclude PCAP extraction and file I/O from timing. |
| UDP PCAP replay | A separate sender transmits the extracted payloads to the A/B receive sockets, paced by capture timestamps or a configured rate. | The socket listeners and ingress queues as well as the downstream pipeline. The OS constructs new packets; the sender does not inject captured Ethernet frames. |
| Synthetic fixtures | Feed decoded events straight into the book or inject constructed MoldUDP64 datagrams into the arbiter. | Deterministic Spock correctness tests with no capture file, socket, or reliance on wall-clock timing. |

Use synthetic book events to assert `100 -> 60 -> gone` after each
`Add -> Execute -> Delete` step. For arbitration, test duplicate copies,
a gap on one leg filled by the other, and a missing sequence on both legs.
Assert that each admitted message is applied once and in sequence, and that
an unresolved gap marks the book stale.

## Benchmark sketch

Use a separate sender to drive the same MoldUDP64 message mix, offered rates,
and burst patterns on the same Linux receiver hardware. Use the same timestamp
boundaries across paths; do not call a receiver-side measurement
NIC-to-publication unless it actually starts at the NIC. Report per-update
p50, p99, and p99.9 latency to book publication alongside achieved throughput,
packet drops, and MoldUDP64 sequence gaps. Also record CPU usage, allocation
rate, and GC activity. A lower tail latency isn't an improvement if it comes
from dropping updates during bursts.

For dual-leg runs, keep the same [A/B fan-in and polling policy](market-data-workflow.md#a-b-feed-arbitration)
across adapters, including queue capacity and poll batch size. Vary which leg
delivers first so the workload doesn't assume a permanently faster feed.

Use portable JVM benchmarks for the two [implementation levers](market-data-workflow.md#swappable-implementations):

- **Mapper handoff:** Hold the book fixed. Compare a bounded
  `LinkedBlockingQueue` using `offer` and `poll` with fresh DTOs, an SPSC ring
  with fresh DTOs, and an SPSC ring with mapping directly into preallocated
  slots. At matched offered load, queue capacity, and batch size, report tail
  handoff latency, allocation per transferred event, and queue-full events.
- **Book storage:** Hold the handoff fixed. Feed the
  [`HashMap` plus `TreeMap` reference and the primitive-array book](market-data-workflow.md#order-book-partial-executions-and-price-levels)
  identical events. Start with `A`, `E`, and `D`; distinguish quantity-only
  updates from executions or deletes that empty the best level and force the
  book to find the next occupied price. Compare update latency and allocation
  across active-order counts.

Use fixed replay to check that each variant applies the same sequence and
produces the same book updates before comparing performance.

Run the first benchmarks without auditing. If a
[cold-path audit journal](market-data-workflow.md#cold-path-audit-journal)
is added later, use the same audit settings across ingress paths. Report
journal lag and audit gaps separately from publication latency, packet drops,
and feed sequence gaps.

Keep historical raw ITCH replay results separate: those files contain no
MoldUDP64 headers and exercise decoding and the book, not A/B arbitration or
the socket and NIC paths.
