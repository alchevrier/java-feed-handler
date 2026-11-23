package io.github.alchevrier.feed.app

import io.github.alchevrier.feed.book.OrderBook
import io.github.alchevrier.feed.book.PackedOrderBook
import io.github.alchevrier.feed.book.ReferenceOrderBook
import io.github.alchevrier.feed.core.MarketMessage
import io.github.alchevrier.feed.nasdaq.ItchBookHandoff
import io.github.alchevrier.feed.nasdaq.ItchEventTarget
import io.github.alchevrier.feed.nasdaq.ItchMapper
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.RawMessageSink
import io.github.alchevrier.feed.nasdaq.MoldUdp64Packet
import io.github.alchevrier.feed.nasdaq.MoldUdp64PacketAllocator
import spock.lang.Specification

import java.nio.ByteBuffer
import java.util.concurrent.Callable
import java.util.concurrent.CountDownLatch
import java.util.concurrent.ExecutionException
import java.util.concurrent.FutureTask
import java.util.concurrent.Semaphore

import static io.github.alchevrier.feed.nasdaq.ItchBookHandoff.StaleReason.BOOK_FAILURE
import static io.github.alchevrier.feed.nasdaq.ItchBookHandoff.StaleReason.DECODE_FAILURE
import static io.github.alchevrier.feed.nasdaq.ItchBookHandoff.StaleReason.GAP_TIMEOUT
import static io.github.alchevrier.feed.nasdaq.ItchBookHandoff.StaleReason.QUEUE_FULL
import static io.github.alchevrier.feed.nasdaq.ItchBookHandoff.StaleReason.REORDER_CAPACITY
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.A
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.B
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.ReorderStorage.POOLED
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.StaleReason.SINK_FAILURE
import static java.nio.charset.StandardCharsets.US_ASCII
import static java.util.concurrent.TimeUnit.SECONDS

class InMemoryItchPipelineSpec extends Specification {
    private static final byte[] SESSION = "SESSION001".getBytes(US_ASCII)
    private static final int LOCATE = 7

    def "applies one ordered A/E/D trace with #layout book and #storage events"() {
        given:
        def arbiter = arbiter()
        def mapper = new ItchMapper(LOCATE)
        def book = book(layout)
        def sequences = []
        def snapshots = []
        def record = { MarketMessage event ->
            assert book.apply(event)
            snapshots << book.topOfBook().bid()
        }
        def target = storage == 'heap' ? ItchEventTarget.heap(record) : ItchEventTarget.reusable(record)
        RawMessageSink sink = { sequence, leg, source, offset, length ->
            mapper.decode(source, offset, length, target)
            sequences << sequence
        }
        def execute = wire('E', 31).putInt(19, 40)
        def delete = wire('D', 19)

        when:
        arbiter.acceptAndForward(B, packet(1003, execute, delete), 0, sink)

        then:
        sequences.empty
        snapshots.empty

        when:
        arbiter.acceptAndForward(A, packet(1000, add(), wire('D', 19, 8), wire('S', 12, 0)), 1, sink)
        arbiter.acceptAndForward(A, packet(1003, execute, delete), 2, sink)

        then:
        sequences == (1000L..1004L).toList()
        snapshots == [new OrderBook.PriceLevel(100_000L, 100), new OrderBook.PriceLevel(100_000L, 60), null]
        book.remainingShares(42) == 0
        arbiter.nextExpectedSequence() == 1005
        arbiter.stale().empty

        where:
        layout << ['reference', 'packed']
        combined:
        storage << ['heap', 'reusable']
    }

    def "#layout book stops before later messages after #scenario"() {
        given:
        def arbiter = arbiter()
        def mapper = new ItchMapper(LOCATE)
        def book = book(layout)
        def target = ItchEventTarget.reusable({ MarketMessage event -> book.apply(event) })
        RawMessageSink sink = { sequence, leg, source, offset, length ->
            mapper.decode(source, offset, length, target)
        }

        when:
        arbiter.acceptAndForward(A, packet(1000, add(), invalid, wire('D', 19)), 0, sink)

        then:
        thrown(error)
        def stale = arbiter.stale().orElseThrow()
        stale.reason() == SINK_FAILURE
        stale.missingSequence() == 1001
        stale.lastEmittedSequence().asLong == 1000
        book.remainingShares(42) == 100

        when:
        arbiter.acceptAndForward(B, packet(1001, wire('D', 19)), 1, sink)

        then:
        book.remainingShares(42) == 100
        arbiter.nextExpectedSequence() == 1001

        where:
        layout << ['reference', 'packed']
        combined:
        scenario               | invalid       | error
        'unsupported mutation' | wire('X', 23) | UnsupportedOperationException
        'malformed execution'  | wire('E', 30) | IllegalArgumentException
    }

    def "bounded handoff preserves transport order through slot reuse with the #layout book"() {
        given:
        def handoff = new ItchBookHandoff(arbiter(), new ItchMapper(LOCATE), 2)
        OrderBook book = book(layout)
        def sequences = []
        def snapshots = []
        ItchBookHandoff.BookEventConsumer apply = { long sequence, MarketMessage event ->
            book.apply(event)
            sequences << sequence
            snapshots << book.topOfBook().bid()
        }

        when:
        handoff.accept(B, packet(1002, wire('E', 31).putInt(19, 40)), 0)

        then:
        !handoff.tryConsume(apply)

        when:
        def datagram = packet(1000, add(), wire('D', 19, 8))
        handoff.accept(A, datagram, 1)
        datagram.putInt(20 + 2 + 20, 999)
        handoff.accept(A, packet(1002, wire('E', 31).putInt(19, 55)), 2)

        then:
        handoff.tryConsume(apply)

        when:
        handoff.accept(B, packet(1003, wire('D', 19)), 3)

        then:
        handoff.tryConsume(apply)
        handoff.tryConsume(apply)
        !handoff.tryConsume(apply)
        handoff.stale().empty
        sequences == [1000L, 1002L, 1003L]
        snapshots == [new OrderBook.PriceLevel(100_000L, 100), new OrderBook.PriceLevel(100_000L, 60), null]

        where:
        layout << ['reference', 'packed']
    }

    def "a consumer holding an add keeps its slot when later adds fill the ring"() {
        given:
        def arbiter = arbiter()
        def handoff = new ItchBookHandoff(arbiter, new ItchMapper(LOCATE), 2)
        def borrowed = new CountDownLatch(1)
        def resume = new CountDownLatch(1)
        def consumer = new FutureTask({
            def observed = []
            ItchBookHandoff.BookEventConsumer hold = { long sequence, MarketMessage.Add event ->
                borrowed.countDown()
                assert resume.await(5, SECONDS)
                observed = [sequence, event.orderReference(), event.side(), event.shares(), event.priceUnits()]
            }
            while (!handoff.tryConsume(hold)) {
                assert !Thread.currentThread().isInterrupted()
                Thread.onSpinWait()
            }
            assert !handoff.tryConsume { sequence, event -> assert false: 'stale run drained its queue' }
            observed
        } as Callable)
        Thread.ofPlatform().daemon().start(consumer)

        when:
        handoff.accept(A, packet(1000, add()), 0)
        assert borrowed.await(5, SECONDS)
        def different = add().putLong(11, 99L).put(19, (byte) 'S').putInt(20, 200).putInt(32, 500_000)
        handoff.accept(B, packet(1001, different), 1)
        assert handoff.stale().empty
        handoff.accept(A, packet(1002, wire('S', 12, 0), different, wire('D', 19)), 2)

        then:
        handoff.stale().orElseThrow().with {
            reason() == QUEUE_FULL && sequence() == 1003
        }
        arbiter.nextExpectedSequence() == 1003

        when:
        resume.countDown()

        then:
        consumer.get(5, SECONDS) == [1000L, 42L, MarketMessage.Side.BUY, 100L, 100_000L]

        when:
        handoff.accept(B, packet(1003, different), 3)

        then:
        handoff.stale().orElseThrow().sequence() == 1003
        arbiter.nextExpectedSequence() == 1003

        cleanup:
        resume.countDown()
        consumer.cancel(true)
    }

    def "one slot passes every field across threads through repeated reuse"() {
        given:
        def handoff = new ItchBookHandoff(arbiter(), new ItchMapper(LOCATE), 1)
        def released = new Semaphore(0)
        int count = 128
        def consumer = new FutureTask({
            def observed = []
            ItchBookHandoff.BookEventConsumer capture = { long sequence, MarketMessage.Add event ->
                observed << [sequence, event.orderReference(), event.side(), event.shares(), event.priceUnits()]
            }
            count.times {
                while (!handoff.tryConsume(capture)) {
                    assert !Thread.currentThread().isInterrupted()
                    assert handoff.stale().empty
                    Thread.onSpinWait()
                }
                released.release()
            }
            observed
        } as Callable)
        Thread.ofPlatform().daemon().start(consumer)

        when:
        count.times { int i ->
            def update = add().putLong(11, -1L - i).put(19, (byte) (i % 2 == 0 ? 'B' : 'S'))
                    .putInt(20, i + 1).putInt(32, 100_000 + i)
            handoff.accept(A, packet(1000L + i, update), i)
            assert released.tryAcquire(5, SECONDS)
        }

        then:
        consumer.get(5, SECONDS) == (0..<count).collect { int i ->
            [1000L + i, -1L - i, i % 2 == 0 ? MarketMessage.Side.BUY : MarketMessage.Side.SELL,
             1L + i, 100_000L + i]
        }
        handoff.stale().empty

        cleanup:
        consumer.cancel(true)
    }

    def "a reentrant book callback cannot consume a second event"() {
        given:
        def handoff = new ItchBookHandoff(arbiter(), new ItchMapper(LOCATE), 2)
        handoff.accept(A, packet(1000, add(), wire('D', 19)), 0)

        when:
        handoff.tryConsume { sequence, event ->
            handoff.tryConsume { nestedSequence, nestedEvent -> assert false: 'nested callback ran' }
        }

        then:
        thrown(IllegalStateException)
        handoff.stale().orElseThrow() == new ItchBookHandoff.Stale(BOOK_FAILURE, 1000)
        !handoff.tryConsume { sequence, event -> assert false: 'stale run drained its queue' }
    }

    def "the first recorded failure wins when #firstFailure happens first"() {
        given:
        def borrowed = new CountDownLatch(1)
        def failBook = new CountDownLatch(1)
        def bookReturned = new CountDownLatch(1)
        def pool = MoldUdp64PacketAllocator.pooled(1)
        int releases = 0
        def allocator = Stub(MoldUdp64PacketAllocator) {
            tryAcquire() >> { pool.tryAcquire() }
            release(_ as MoldUdp64Packet) >> { MoldUdp64Packet descriptor ->
                pool.release(descriptor)
                // On the second datagram, the arbiter has failed but the handoff has not yet mirrored it.
                if (++releases == 2 && firstFailure == BOOK_FAILURE) {
                    failBook.countDown()
                    assert bookReturned.await(5, SECONDS)
                }
            }
        }
        def handoff = new ItchBookHandoff(arbiter(allocator), new ItchMapper(LOCATE), 1)
        handoff.accept(A, packet(1000, wire('D', 19)), 0)
        def consumer = new FutureTask({
            OrderBook book = new ReferenceOrderBook()
            try {
                handoff.tryConsume { sequence, event ->
                    borrowed.countDown()
                    assert failBook.await(5, SECONDS)
                    book.apply(event)
                }
            } finally {
                bookReturned.countDown()
            }
        } as Callable)
        Thread.ofPlatform().daemon().start(consumer)

        when:
        assert borrowed.await(5, SECONDS)
        handoff.accept(B, packet(1001, wire('X', 23)), 1)

        then:
        thrown(UnsupportedOperationException)
        handoff.stale().orElseThrow() == new ItchBookHandoff.Stale(firstFailure, firstSequence)

        when:
        failBook.countDown()
        consumer.get(5, SECONDS)

        then:
        ExecutionException consumerFailure = thrown()
        consumerFailure.cause instanceof IllegalArgumentException
        handoff.stale().orElseThrow() == new ItchBookHandoff.Stale(firstFailure, firstSequence)

        cleanup:
        failBook.countDown()
        consumer.cancel(true)

        where:
        firstFailure   | firstSequence
        DECODE_FAILURE | 1001L
        BOOK_FAILURE   | 1000L
    }

    def "bad ITCH marks the handoff stale without applying queued work"() {
        given:
        def handoff = new ItchBookHandoff(arbiter(), new ItchMapper(LOCATE), 2)

        when:
        handoff.accept(A, packet(1000, add(), wire('X', 23)), 0)

        then:
        thrown(UnsupportedOperationException)
        handoff.stale().orElseThrow().with {
            reason() == DECODE_FAILURE && sequence() == 1001
        }
        !handoff.tryConsume { sequence, event -> assert false }
    }

    def "a #scenario publishes terminal status outside the ring"() {
        given:
        def handoff = new ItchBookHandoff(arbiter(), new ItchMapper(LOCATE), 1)
        handoff.accept(A, packet(1000, add()), 0)

        when:
        int count = scenario == 'gap timeout' ? 1 : 3
        handoff.accept(B, packet(1002, *((1..count).collect { wire('D', 19) })), 1)
        handoff.checkTimeout(11)

        then:
        handoff.stale().orElseThrow().with {
            reason() == reasonExpected && sequence() == 1001
        }
        !handoff.tryConsume { sequence, event -> assert false }

        where:
        scenario              | reasonExpected
        'gap timeout'         | GAP_TIMEOUT
        'reorder exhaustion'  | REORDER_CAPACITY
    }

    def "a rejected book update marks the handoff stale at that event sequence"() {
        given:
        def handoff = new ItchBookHandoff(arbiter(), new ItchMapper(LOCATE), 2)
        OrderBook book = new ReferenceOrderBook()
        handoff.accept(A, packet(1000, wire('D', 19), add()), 0)

        when:
        handoff.tryConsume { sequence, event -> book.apply(event) }

        then:
        thrown(IllegalArgumentException)
        handoff.stale().orElseThrow().with {
            reason() == BOOK_FAILURE && sequence() == 1000
        }
        !handoff.tryConsume { sequence, event -> assert false }
        book.remainingShares(42) == 0
    }

    def "ring rejects capacity #capacity"() {
        when:
        new ItchBookHandoff(arbiter(), new ItchMapper(LOCATE), capacity)

        then:
        thrown(IllegalArgumentException)

        where:
        capacity << [-1, 0, 3]
    }

    private static MoldUdp64Arbiter arbiter(MoldUdp64PacketAllocator allocator = MoldUdp64PacketAllocator.pooled(1)) {
        new MoldUdp64Arbiter(SESSION, 1000, 2, 10, allocator, POOLED)
    }

    private static OrderBook book(String layout) {
        if (layout == 'reference') {
            return new ReferenceOrderBook()
        }
        return new PackedOrderBook(4, 4)
    }

    private static ByteBuffer add() {
        wire('A', 36).put(19, (byte) 'B').putInt(20, 100).putInt(32, 100_000)
    }

    private static ByteBuffer wire(String type, int length, int locate = LOCATE) {
        def payload = ByteBuffer.allocate(length).put(0, (byte) type.charAt(0)).putShort(1, (short) locate)
        if (length >= 19) {
            payload.putLong(11, 42L)
        }
        payload
    }

    private static ByteBuffer packet(long sequence, ByteBuffer... messages) {
        int size = 20 + (messages.sum { 2 + it.remaining() } as int)
        def datagram = ByteBuffer.allocate(size).put(SESSION).putLong(sequence).putShort((short) messages.length)
        messages.each { message ->
            datagram.putShort((short) message.remaining()).put(message.duplicate())
        }
        datagram.flip()
    }
}
