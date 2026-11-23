package io.github.alchevrier.feed.app

import io.github.alchevrier.feed.book.OrderBook
import io.github.alchevrier.feed.book.PackedOrderBook
import io.github.alchevrier.feed.book.ReferenceOrderBook
import io.github.alchevrier.feed.core.MarketMessage
import io.github.alchevrier.feed.nasdaq.ItchEventTarget
import io.github.alchevrier.feed.nasdaq.ItchMapper
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.RawMessageSink
import io.github.alchevrier.feed.nasdaq.MoldUdp64PacketAllocator
import spock.lang.Specification

import java.nio.ByteBuffer

import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.A
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.B
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.ReorderStorage.POOLED
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.StaleReason.SINK_FAILURE
import static java.nio.charset.StandardCharsets.US_ASCII

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

    private static MoldUdp64Arbiter arbiter() {
        new MoldUdp64Arbiter(SESSION, 1000, 2, 10, MoldUdp64PacketAllocator.pooled(1), POOLED)
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
