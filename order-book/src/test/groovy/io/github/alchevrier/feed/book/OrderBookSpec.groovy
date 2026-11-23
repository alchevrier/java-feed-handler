package io.github.alchevrier.feed.book

import io.github.alchevrier.feed.core.MarketMessage
import spock.lang.Specification

import static io.github.alchevrier.feed.book.OrderBook.PriceLevel
import static io.github.alchevrier.feed.book.OrderBook.TopOfBook
import static io.github.alchevrier.feed.book.OrderBook.TopOfBookBuffer
import static io.github.alchevrier.feed.core.MarketMessage.*
import static io.github.alchevrier.feed.core.MarketMessage.Side.*

class OrderBookSpec extends Specification {
    def "#layout: #scenario"() {
        given:
        OrderBook book = newBook(layout)
        def buffer = new TopOfBookBuffer()
        def previous = top(null, null)

        expect:
        book.topOfBook() == top(null, null)
        verifyEach(steps) { step ->
            assert book.apply(step.message) == step.changed
            assert book.remainingShares(step.message.orderReference()) == step.remaining
            assert book.topOfBook() == step.snapshot
            book.readTopOfBook(buffer)
            def captured = buffer.snapshot(previous)
            assert captured == step.snapshot
            assert captured.is(previous) == !step.changed
            return (previous = captured)
        }

        where:
        layout << ["reference", "packed"]
        combined:
        [scenario, steps] << [
            ["add, partial execution, delete", [
                step(add(42L, BUY, 100L, 100_000L), true, 100, top(level(100_000, 100), null)),
                step(execute(42L, 40L), true, 60, top(level(100_000, 60), null)),
                step(delete(42L), true, 0, top(null, null))
            ]],
            ["aggregate a shared level and expose the next price after a full fill", [
                step(add(1L, BUY, 10L, 99L), true, 10, top(level(99, 10), null)),
                step(add(2L, BUY, 20L, 100L), true, 20, top(level(100, 20), null)),
                step(add(3L, BUY, 30L, 100L), true, 30, top(level(100, 50), null)),
                step(delete(2L), true, 0, top(level(100, 30), null)),
                step(execute(1L, 5L), false, 5, top(level(100, 30), null)),
                step(execute(3L, 30L), true, 0, top(level(99, 5), null))
            ]],
            ["maintain asks in ascending order and keep bids separate", [
                step(add(-1L, SELL, 10L, 102L), true, 10, top(null, level(102, 10))),
                step(add(Long.MIN_VALUE, SELL, 20L, 101L), true, 20, top(null, level(101, 20))),
                step(add(0L, BUY, 30L, 0L), true, 30, top(level(0, 30), level(101, 20))),
                step(delete(Long.MIN_VALUE), true, 0, top(level(0, 30), level(102, 10)))
            ]],
            ["clear both sides on a reused quote buffer", [
                step(add(1L, BUY, 5L, 0L), true, 5, top(level(0, 5), null)),
                step(add(2L, SELL, 8L, 101L), true, 8, top(level(0, 5), level(101, 8))),
                step(delete(1L), true, 0, top(null, level(101, 8))),
                step(delete(2L), true, 0, top(null, null))
            ]],
            ["preserve resting orders when inserting and removing interior price levels", [
                step(add(1L, BUY, 10L, 100L), true, 10, top(level(100, 10), null)),
                step(add(2L, BUY, 20L, 90L), false, 20, top(level(100, 10), null)),
                step(add(3L, BUY, 30L, 95L), false, 30, top(level(100, 10), null)),
                step(execute(2L, 5L), false, 15, top(level(100, 10), null)),
                step(delete(3L), false, 0, top(level(100, 10), null)),
                step(delete(1L), true, 0, top(level(90, 15), null))
            ]]
        ]
    }

    def "#layout rejects #scenario without altering the book"() {
        given:
        OrderBook book = newBook(layout)
        book.apply(add(1L, BUY, 10L, 100L))
        def before = book.topOfBook()

        when:
        book.apply(invalid)

        then:
        thrown(IllegalArgumentException)
        book.topOfBook() == before
        book.remainingShares(1L) == 10
        book.remainingShares(2L) == 0

        where:
        layout << ["reference", "packed"]
        combined:
        scenario                  | invalid
        "duplicate add"           | add(1L, SELL, 20L, 101L)
        "unknown execution"       | execute(2L, 1L)
        "unknown delete"          | delete(2L)
        "overfill"                | execute(1L, 11L)
        "zero execution in slot"  | new ExecutionSlot(1L, 0L)
        "missing side in slot"    | new AddSlot(2L, null, 20L, 101L)
        "negative price in slot"  | new AddSlot(2L, SELL, 20L, -1L)
        "zero add shares in slot" | new AddSlot(2L, SELL, 0L, 101L)
    }

    def "#layout rejects overflowing level totals atomically"() {
        given:
        OrderBook book = newBook(layout)
        book.apply(add(1L, BUY, Long.MAX_VALUE, 100L))

        when:
        book.apply(add(2L, BUY, 1L, 100L))

        then:
        thrown(ArithmeticException)
        book.remainingShares(2L) == 0
        book.topOfBook() == top(level(100, Long.MAX_VALUE), null)

        where:
        layout << ["reference", "packed"]
    }

    def "#layout does not retain event slots or mutate prior snapshots"() {
        given:
        OrderBook book = newBook(layout)
        def slot = new AddSlot(1L, BUY, 10L, 100L)
        book.apply(slot)
        def before = book.topOfBook()

        when:
        slot.reference = 2L
        slot.side = SELL
        slot.quantity = 20L
        slot.price = 101L
        book.apply(slot)
        book.apply(execute(1L, 5L))

        then:
        book.remainingShares(1L) == 5
        book.remainingShares(2L) == 20
        before == top(level(100, 10), null)
        book.topOfBook() == top(level(100, 5), level(101, 20))

        where:
        layout << ["reference", "packed"]
    }

    def "packed book constructor rejects #scenario"() {
        when:
        new PackedOrderBook(maxOrders, maxLevelsPerSide)

        then:
        thrown(IllegalArgumentException)

        where:
        scenario                           | maxOrders       | maxLevelsPerSide
        "zero order capacity"              | 0               | 1
        "order capacity above the maximum" | (1 << 29) + 1   | 1
        "zero per-side level capacity"     | 1               | 0
    }

    def "packed book rejects #scenario and can reuse capacity after removal"() {
        given:
        OrderBook book = new PackedOrderBook(maxOrders, maxLevels)
        book.apply(add(1L, BUY, 10L, 100L))
        def before = book.topOfBook()

        when:
        book.apply(second)

        then:
        thrown(IllegalStateException)
        book.topOfBook() == before
        book.remainingShares(1L) == 10
        book.remainingShares(second.orderReference()) == 0

        when:
        book.apply(removeFirst)
        book.apply(second)

        then:
        book.remainingShares(1L) == 0
        book.remainingShares(second.orderReference()) == second.shares()

        where:
        scenario       | maxOrders | maxLevels | second                  | removeFirst
        "order limit"  | 1         | 2         | add(2L, BUY, 20L, 100L) | delete(1L)
        "level limit"  | 2         | 1         | add(2L, BUY, 20L, 101L) | execute(1L, 10L)
    }

    def "packed book counts only distinct prices toward each side's level capacity"() {
        given:
        OrderBook book = new PackedOrderBook(4, 1)

        expect:
        book.apply(add(0L, BUY, 10L, 100L))
        book.apply(add(-1L, BUY, 20L, 100L))
        book.apply(add(Long.MIN_VALUE, SELL, 30L, 100L))
        book.topOfBook() == top(level(100, 30), level(100, 30))

        when:
        book.apply(add(1L, SELL, 40L, 101L))

        then:
        thrown(IllegalStateException)
        book.topOfBook() == top(level(100, 30), level(100, 30))
        book.remainingShares(1L) == 0
    }

    def "packed book reuses order slots and preserves lookups under churn"() {
        given:
        OrderBook reference = new ReferenceOrderBook()
        OrderBook packed = new PackedOrderBook(17, 17)
        def random = new Random(42L)
        def references = (0..<17).collect { random.nextLong() }
        def events = references.collect { add(it, BUY, 10L, 100L) }
        Set<Long> active = new HashSet<>()
        500.times { i ->
            int position = random.nextInt(references.size())
            events << execute(references[position], 5L)
            events << delete(references[position])
            references[position] = random.nextLong()
            events << add(references[position], i % 2 == 0 ? BUY : SELL, 10L, (long) (90 + i % 23))
        }

        expect:
        verifyEach(events) { message ->
            assert packed.apply(message) == reference.apply(message)
            assert packed.topOfBook() == reference.topOfBook()
            long id = message.orderReference()
            if (reference.remainingShares(id) == 0) {
                assert active.remove(id)
                assert packed.remainingShares(id) == 0
            } else if (message instanceof MarketMessage.Add) {
                assert active.add(id)
            }
            for (long live : active) {
                assert packed.remainingShares(live) == reference.remainingShares(live)
            }
        }
    }

    private static OrderBook newBook(String layout) {
        layout == "reference" ? new ReferenceOrderBook() : new PackedOrderBook(8, 8)
    }

    private static Map step(MarketMessage message, boolean changed, long remaining, TopOfBook snapshot) {
        [message: message, changed: changed, remaining: remaining, snapshot: snapshot]
    }

    private static PriceLevel level(long price, long shares) {
        new PriceLevel(price, shares)
    }

    private static TopOfBook top(PriceLevel bid, PriceLevel ask) {
        new TopOfBook(bid, ask)
    }

    private record ExecutionSlot(long orderReference, long executedShares) implements MarketMessage.Execute {}

    private static class AddSlot implements MarketMessage.Add {
        long reference
        MarketMessage.Side side
        long quantity
        long price

        AddSlot(long reference, MarketMessage.Side side, long quantity, long price) {
            this.reference = reference
            this.side = side
            this.quantity = quantity
            this.price = price
        }

        long orderReference() { reference }
        MarketMessage.Side side() { side }
        long shares() { quantity }
        long priceUnits() { price }
    }
}
