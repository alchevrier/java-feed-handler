# ADR-001: Reconstruct an A/E/D order book before optimizing it

**Status: Proposed**

## Context

The first deliverable should prove that the JVM can reconstruct a displayed
book from a deterministic event trace. It should not depend on UDP, a capture
file, or a particular storage optimization.

## Proposed decision

Use the build toolchain proposed in [ADR-004](ADR-004-jdk25-gradle-monorepo.md).
Define the book's observable contract first with data-driven Spock specs.
Start with one instrument and
one writer. Use a `HashMap<Long, Order>` for order lookup and a `TreeMap`
per side for aggregated price levels. Keep prices as fixed-point integers
and use `long` for level quantities.

## Nasdaq TotalView-ITCH 5.0 messages

Nasdaq reports changes to its displayed book; the handler applies those
changes rather than matching orders itself. MoldUDP64 provides framing and
message sequence numbers. Each message block wraps an ITCH message whose
first byte identifies the action. `A`, `E`, and `D` are **ITCH types**, not
MoldUDP64 packet types. [ADR-002](ADR-002-portable-udp-feed.md#nasdaq-moldudp64-framing)
defines the transport; this ADR defines what the decoded events mean for the book.

### Wire reference

The layouts below follow the official [Nasdaq TotalView-ITCH 5.0
specification][itch], sections 1.3.1, 1.4.1, and 1.4.4. Offsets are
zero-based from the ITCH type byte. Lengths exclude the MoldUDP64 header
and the per-message length prefix. Numeric fields are unsigned, big-endian
binary values.

| Offset | Bytes | Field shared by `A`, `E`, and `D` |
|---:|---:|---|
| 0 | 1 | ASCII message type: `A`, `E`, or `D`. |
| 1 | 2 | Stock locate: Nasdaq's code identifying the security. |
| 3 | 2 | Tracking number: Nasdaq internal tracking field, not the transport sequence. |
| 5 | 6 | Timestamp: nanoseconds since midnight. |
| 11 | 8 | Order reference: identifies the new order in `A` and the existing order in `E` and `D`. |

| Message | Total bytes | Fields after the shared 19 bytes |
|---|---:|---|
| `A`: Add Order without MPID attribution | 36 | Side at offset 19 (1 byte, ASCII `B` for buy or `S` for sell); shares at 20 (4 bytes); stock symbol at 24 (8 bytes, ASCII padded on the right with spaces); display price at 32 (4 bytes, scaled by 10,000). This form carries no market participant identifier (MPID). |
| `E`: Order Executed | 31 | Executed shares at offset 19 (4 bytes); match number at 23 (8 bytes). The match number identifies the execution, not the resting order. No price is carried. |
| `D`: Order Delete | 19 | None. The message carries no quantity or price. |

### Applying the messages

`A` creates a resting order and increases the aggregate size at its side and
price. `E` looks up that order by reference and subtracts the executed
shares from both the order and its resting price level. Executions are
cumulative; remove the order when its remaining quantity reaches zero.
`D` looks up the order, subtracts **all remaining shares** from its
resting level, and removes the order.

| Step | Nasdaq message for order `42` | Expected book state |
|---|---|---|
| 1 | `A`: buy 100 shares at $10.00 (wire price `100000`) | Order `42` rests for 100 shares; best bid is $10.00 for 100. |
| 2 | `E`: execute 40 shares | Order `42` rests for 60 shares at the same price; best bid is $10.00 for 60. The `40` is a delta, not the new size. |
| 3 | `D`: delete order `42` | Remove the remaining 60 shares. Since this example has no other bids, the best bid is now absent. |

Remove a level only when its aggregate quantity reaches zero. Publish a
top-of-book change when the best price or displayed size changes; consumers
do not read mutable book state. Reject a duplicate live order reference, an
execution or delete for an unknown order, or an execution outside the
remaining quantity without mutating the book.

Run the same behavioral contract against later implementations. This is a
subset of displayed-book reconstruction: `F` (attributed Add Order),
`C` (Order Executed With Price), `X` (Order Cancel for a stated share
quantity), and `U` (Order Replace with a new reference) also affect the book
but are deferred. Multi-instrument state and order matching are out of scope.

## Alternatives and tradeoffs

- **Primitive arrays first:** Avoids some object and lookup overhead but
  requires slot management and a price-level layout before a reference exists.
- **`ConcurrentHashMap`:** Adds a concurrent lookup capability this workflow
  does not need and still would not make an order update and its level update
  atomic together.

## Acceptance criteria

Assert state and top-of-book after each step of `100 -> 60 -> gone`. Also
cover a full fill, multiple orders at one level, removal of the last order at
the best price with another level available, a fill away from the best,
and invalid events that leave state unchanged. `./gradlew build` on JDK 25
must pass the full test suite.

Source: [Order-book draft](../draft/market-data-workflow.md#order-book-partial-executions-and-price-levels).

[itch]: https://www.nasdaqtrader.com/content/technicalsupport/specifications/dataproducts/NQTVITCHSpecification.pdf
