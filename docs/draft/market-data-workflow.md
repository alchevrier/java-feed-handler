# ITCH replay and book reconstruction

**Status: Draft.** This is a working note, not an accepted ADR.

The exchange matches orders. The feed handler reconstructs the visible order
book by applying exchange messages in sequence.

| ITCH event | Effect on the reconstructed book |
|---|---|
| Add a buy order for 100 shares at $10 | Record the order by its reference number; 100 shares are resting. |
| Execute 40 shares of that order | Reduce its remaining quantity to 60 shares. |
| Delete the order | Remove the remaining 60 shares. |

Historical ITCH file replay reads length-prefixed messages without MoldUDP64
headers. A small recording of this example can assert `100 -> 60 -> gone` before
networking is involved. Both replay and live input should exercise the same ITCH
decoder and book logic.

First compose the portable UDP-to-snapshot workflow and verify its
correctness with fixed input. Then benchmark the reference pipeline,
isolate component and handoff costs, and rerun the full path to see which
changes carry through. See [ADR-003](../adr/ADR-003-performance-baselines.md)
for the proposed measurement sequence. A live audit journal is not a
prerequisite.
See [Replay and correctness inputs](ingress-comparison.md#replay-and-correctness-inputs)
for the two PCAP replay modes and synthetic Spock fixtures.

## Build toolchain

Use the Gradle Wrapper and a Java toolchain targeting JDK 25 for compilation,
tests, and JVM benchmarks on macOS and Linux. Pin the wrapper to a Gradle
release that can run on JDK 25. See [ADR-004](../adr/ADR-004-jdk25-gradle-monorepo.md)
for the proposed toolchain and monorepo module boundaries.

## Portable UDP thread topology

```text
UDP receiver A -> Queue A --\
                             +-> Arbiter -> ITCH mapper -> Queue -> Order book
UDP receiver B -> Queue B --/                                        |
                                                                     +-> Snapshot publication -> Strategy
```

Use one Java platform thread per UDP receiver, one for arbitration and
mapping, and one to own the order book. Each bounded queue has one producer
and one consumer (SPSC), regardless of implementation. The two ingress queues
form an MPSC fan-in without a shared producer queue. A separate downstream
queue carries one arbitrated stream from the mapper to the single book writer.
The topology works on macOS and Linux without CPU pinning or kernel bypass.

The arbiter emits ordered raw ITCH messages. The mapper decodes each admitted
message and publishes a sequence-tagged book event to the downstream queue
before releasing the ingress storage. Surface a full queue explicitly; do
not silently skip an admitted event and continue as if the book received it.

## Swappable implementations

Keep one workflow: live ingress, A/B arbitration, ITCH decoding, book update,
and snapshot publication. Compose the implementations at startup. Interfaces
at those boundaries let the reference and tuned versions share ordering and
publication rules without sharing a transport or storage layout.

`Queue` names the handoff boundary, not a required `java.util.Queue` API.
A bounded `LinkedBlockingQueue` is a reference implementation: use `offer`
and `poll` rather than waiting for space or data with `put` and `take`.
`offer` and `poll` still use locks. A specialized SPSC ring is the tuned
candidate.

The handoff lifecycle is `claim -> write -> publish -> consume -> release`.
The mapper can write into a fresh event object (DTO) and enqueue it, or claim
an SPSC slot and map into it directly without an intermediate DTO. Both
handoffs preserve FIFO order, report capacity exhaustion explicitly, and
keep the data valid until release. The producer stops mutating the data at
publication, and the consumer stops using it at release.

Where object allocation is a separate concern, both implementations can use
a typed `tryAcquire` / `release` contract. `NewAllocator` creates an object
and does nothing on release; `SlotAllocator` lends a preallocated object and
reports exhaustion instead of allocating. The caller follows the same
ownership rules with either. If a ring owns its slots, it can provide both
queueing and storage behind the handoff interface without a separate allocator
or an extra copy.

## Order book: partial executions and price levels

The first order-book deliverable supports **`A -> E -> D`**: add a resting
order, execute part or all of it, and delete an order that is still resting.
The first fixture adds 100 shares, executes 40, and deletes the remaining 60.
Also cover deleting the last order at the best level while another level
remains, so the snapshot moves to the next occupied price. `F`, `C`, `X`, and
`U` are later work; this slice is not full ITCH book reconstruction.

The mapper passes the order reference and executed quantity to the book.
Executed quantity is a delta, not the new remaining quantity. If order `42`
rests for 100 shares at $10 and 40 execute, leave the order at $10 with 60
shares and subtract 40 from the total at that price. Remove the order when
its remaining quantity reaches zero. If the **whole price level** empties,
remove it; if it was the best price, find the next occupied level on that
side. Publish a top-of-book update when the best price or its displayed
size changes.

- **Reference book:** Use `HashMap<Long, Order>` to find the resting order
  and a `TreeMap` of price-level totals per side. Remove the `HashMap` entry
  when the order is gone. Remove the `TreeMap` entry only when the total at
  that price reaches zero. The tree gives the highest occupied bid or lowest
  occupied ask without scanning every order.
- **Tuned book:** Use a primitive map from order reference to slot index
  and preallocated primitive arrays for order fields. Order references are
  not array indexes. For price levels, a packed sorted array shifts
  subsequent entries to close the hole when a level empties. A tick-indexed
  array instead clears the fixed slot without shifting and finds the next
  occupied tick by scanning or using a bitmap. The choice of level layout,
  supported tick range, and lookup method remains open.

A partial execution that leaves the level occupied updates quantities in
place in either design; it does not remove a price level or shift entries.

The book thread owns all mutable state. Strategy consumes published snapshots
rather than reading the mutable book. `ConcurrentHashMap` is unnecessary
under this ownership model and would not make the order and level updates
atomic together. Finding the next occupied level keeps the quote current
after a fill, but no storage layout can repair a missing feed message; an
unresolved sequence gap still makes the book stale.

## A/B feed arbitration

Feed A and Feed B carry redundant copies of the same exchange stream. Arbitrate
before updating the book: for the next expected sequence, accept the first
valid copy the consumer sees from either leg. Apply it once and ignore
duplicates. Deduplicate by logical feed channel, session, and **message**
sequence, not by UDP datagram.

The arbiter polls the ingress queues round-robin: take a bounded batch per
turn and skip empty queues. Network
conditions can make either leg deliver a message first, so neither leg gets
permanent priority. Polling order decides where to look, not what sequence to
apply. If both copies are queued, use the first valid copy polled; don't wait
for the other leg or try to reconstruct NIC arrival order.

The [FPGA implementation](https://github.com/alchevrier/fpga-feed-handler/blob/d437613d9d7a7c0aa81d13cdd207d2c2c9d39e17/src/feed_handler.cpp#L7-L33)
is the reference for putting this check before ITCH decoding and book mutation.
It checks A before B and discards any message that doesn't match the expected
sequence, including messages ahead of the expected sequence. Carry forward the
single-stream admission rule, not the FPGA's fixed priority or discard policy.

A MoldUDP64 datagram starting at `1000` with two messages carries `1000` and
`1001`. If A jumps to `1003`, hold it within the reorder window and keep
polling B. If B supplies `1002`, apply `1002` before `1003`.
A gap on one leg is not necessarily a gap in the merged stream. If neither
leg supplies `1002`, don't apply later messages out of order. An unresolved
gap leaves the affected book stale until the missing messages are applied in
order or the book is rebuilt from known-good state. The reorder window and
recovery policy are still open.

Exercise this with MoldUDP64 datagrams on both legs: raw ITCH file replay
cannot test arbitration or transport gap detection. See
[Ingress comparison](ingress-comparison.md) for candidate live receive paths.

## Cold-path audit journal

**Deferred.** The first milestone does not require an audit sink, journal
writer, or checkpoint storage.

The proposed audit handoff follows the book update and any resulting snapshot
publication. The book thread sends a record through a bounded queue so
strategy does not wait for disk writes. A cold-path writer appends
**one record per applied message**, not one per A/B copy. Each
record carries the logical feed channel, session, message sequence number,
and ITCH bytes. The selected leg can be kept as metadata. If a snapshot was
published, pass its values with the record; reading the live book later could
capture a different sequence. The record must own its data before the receive
buffer is reused.

Journal every applied message, even when it doesn't change the published
snapshot. A partial execution below the best bid can leave top-of-book
unchanged while still changing the book. Also record feed gaps, recovery
boundaries, and any replacement snapshot used to rebuild the book. The
journal records what the handler applied; it cannot provide messages that
never arrived.

To replay through sequence `N`, start from the nearest saved full-book
checkpoint at or before `N` (or from session start) and apply subsequent
journal entries through `N`. Compare replayed publications with what was
recorded during the live run. To verify the full book, compare a deterministic
checksum against one captured from the live book at the same checkpoint
sequence. Matching top-of-book does not prove that the entire book matches.

Publication precedes durability: a crash can lose queued records, and the
writer can fall behind. Track the last durably recorded sequence and report
audit gaps separately from feed gaps. Behavior when the queue fills or the
writer fails remains open; if a complete audit trail is mandatory,
publication must not silently continue after audit loss.
