package io.github.alchevrier.feed.nasdaq

import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.RawMessageSink
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.ByteOrder
import java.nio.ReadOnlyBufferException

import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.*
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.StaleReason.*
import static java.nio.charset.StandardCharsets.US_ASCII

class MoldUdp64ArbiterSpec extends Specification {
    private static final String SESSION = "SESSION001"

    def "accepts a datagram with the default allocator"() {
        given:
        def arbiter = new MoldUdp64Arbiter(SESSION.getBytes(US_ASCII), 1000, 1, 10)

        expect:
        describe(arbiter.accept(A, packet(1000, "A"), 0)) == [[1000L, A, "A"]]
        arbiter.nextExpectedSequence() == 1001
    }

    def "constructor rejects #scenario"() {
        when:
        new MoldUdp64Arbiter(session.getBytes(US_ASCII), 1000, capacity, timeoutNanos)

        then:
        thrown(IllegalArgumentException)

        where:
        scenario                 | session   | capacity | timeoutNanos
        "a short session ID"     | "SHORT"   | 1        | 10L
        "zero reorder capacity"  | SESSION   | 0        | 10L
        "zero gap timeout"       | SESSION   | 1        | 0L
    }

    def "sequences individual messages and suppresses overlapping copies when #firstLeg arrives first (pooled: #pooled)"() {
        given:
        def arbiter = newArbiter(pooled)

        expect:
        describe(arbiter.accept(firstLeg, packet(1000, "A", "E"), 0)) ==
            [[1000L, firstLeg, "A"], [1001L, firstLeg, "E"]]
        describe(arbiter.accept(otherLeg, packet(1001, "duplicate", "D"), 1)) ==
            [[1002L, otherLeg, "D"]]
        arbiter.accept(otherLeg, packet(1000, "A", "E", "D"), 2).empty
        arbiter.nextExpectedSequence() == 1003
        arbiter.stale().empty

        where:
        pooled << [false, true]
        combined:
        firstLeg | otherLeg
        A        | B
        B        | A
    }

    def "holds later data until #repairLeg fills the gap, keeping the first buffered copy (pooled: #pooled)"() {
        given:
        def arbiter = newArbiter(pooled)

        expect:
        arbiter.accept(A, packet(1003, "later"), 0).empty
        arbiter.accept(B, packet(1003, "duplicate"), 1).empty
        arbiter.accept(A, packet(1002, "third"), 2).empty
        arbiter.nextExpectedSequence() == 1000
        describe(arbiter.accept(repairLeg, packet(1000, "first", "second", "duplicate"), 3)) ==
            [[1000L, repairLeg, "first"], [1001L, repairLeg, "second"], [1002L, A, "third"], [1003L, A, "later"]]
        arbiter.nextExpectedSequence() == 1004
        arbiter.checkTimeout(100).empty

        where:
        pooled << [false, true]
        combined:
        repairLeg << [A, B]
    }

    def "#control does not consume a message number but can reveal a gap without data (pooled: #pooled)"() {
        given:
        def arbiter = newArbiter(pooled)

        expect:
        arbiter.accept(A, header(SESSION, 1000, count).flip(), 0).empty
        arbiter.checkTimeout(20).empty
        arbiter.nextExpectedSequence() == 1000
        arbiter.accept(B, header(SESSION, 1002, count).flip(), 21).empty
        arbiter.checkTimeout(30).empty
        arbiter.checkTimeout(31).orElseThrow().with {
            reason() == GAP_TIMEOUT && missingSequence() == 1000 && lastEmittedSequence().empty
        }

        where:
        pooled << [false, true]
        combined:
        control          | count
        "heartbeat"      | 0
        "end of session" | 0xffff
    }

    def "timeout is measured from the first evidence and is terminal even with partial progress (pooled: #pooled)"() {
        given:
        def arbiter = newArbiter(pooled)

        expect:
        arbiter.accept(A, packet(1003, "fourth"), 0).empty
        describe(arbiter.accept(B, packet(1000, "first"), 8)) == [[1000L, B, "first"]]
        arbiter.checkTimeout(9).empty
        arbiter.accept(B, packet(1001, "too late"), 10).empty
        arbiter.stale().orElseThrow().with {
            reason() == GAP_TIMEOUT && missingSequence() == 1001 && lastEmittedSequence().asLong == 1000
        }
        arbiter.accept(B, packet(1001, "second", "third"), 11).empty
        arbiter.nextExpectedSequence() == 1001

        where:
        pooled << [false, true]
    }

    def "reorder capacity counts distinct pending messages and fails closed on exhaustion (pooled: #pooled)"() {
        given:
        def arbiter = newArbiter(pooled, 1)

        expect:
        describe(arbiter.accept(A, packet(1000, "first"), 0)) == [[1000L, A, "first"]]
        arbiter.accept(A, packet(1002, "third"), 1).empty
        arbiter.accept(B, packet(1002, "duplicate"), 2).empty
        arbiter.stale().empty
        arbiter.accept(B, packet(1003, "too many"), 3).empty
        arbiter.stale().orElseThrow().with {
            reason() == REORDER_CAPACITY && missingSequence() == 1001 && lastEmittedSequence().asLong == 1000
        }
        arbiter.accept(B, packet(1001, "second"), 4).empty

        where:
        pooled << [false, true]
    }

    def "discard #scenario atomically without advancing the sequence or starting a gap timer (pooled: #pooled)"() {
        given:
        def arbiter = newArbiter(pooled)

        expect:
        arbiter.accept(A, badPacket, 0).empty
        arbiter.malformedDatagrams() == malformed
        arbiter.wrongSessionDatagrams() == 1 - malformed
        arbiter.nextExpectedSequence() == 1000
        arbiter.checkTimeout(100).empty
        describe(arbiter.accept(B, packet(1000, "valid"), 101)) == [[1000L, B, "valid"]]

        where:
        pooled << [false, true]
        combined:
        scenario                       | badPacket                                                       | malformed
        "a short header"               | ByteBuffer.allocate(19)                                         | 1
        "a truncated second block"     | packet(1000, "keep out", "bad").limit(20 + 2 + 8 + 2)             | 1
        "an incomplete length field"  | header(SESSION, 1003, 1).put((byte) 0).flip()                    | 1
        "an unsigned oversized length" | header(SESSION, 1003, 1).putShort((short) -1).flip()              | 1
        "trailing data"                | header(SESSION, 1003, 1).putShort((short) 0).put((byte) 1).flip() | 1
        "an invalid heartbeat"        | header(SESSION, 1003, 0).put((byte) 1).flip()                    | 1
        "a sequence range wrapping 0"  | packet(-1L, "wrap")                                              | 1
        "a different session"          | header("SESSION999", 1003, 0).flip()                             | 0
    }

    def "reads the datagram range in network order and owns delayed and emitted payloads (pooled: #pooled)"() {
        given:
        def arbiter = newArbiter(pooled)
        def incoming = ByteBuffer.allocateDirect(64).order(ByteOrder.LITTLE_ENDIAN)
        incoming.position(3)
        incoming.put(packet(1001, "second")).flip().position(3)
        int limit = incoming.limit()

        when:
        def ahead = arbiter.accept(A, incoming, 0)

        then:
        ahead.empty
        incoming.position() == 3
        incoming.limit() == limit
        incoming.order() == ByteOrder.LITTLE_ENDIAN

        when:
        incoming.put(3 + 22, (byte) 0)
        def messages = arbiter.accept(B, packet(1000, ""), 1)

        then:
        describe(messages) == [[1000L, B, ""], [1001L, A, "second"]]
        describe(messages) == [[1000L, B, ""], [1001L, A, "second"]]

        when:
        messages[1].payload().put((byte) 0)

        then:
        thrown(ReadOnlyBufferException)

        where:
        pooled << [false, true]
    }

    def "orders unsigned sequences across the signed long boundary (pooled: #pooled)"() {
        given:
        def arbiter = new MoldUdp64Arbiter(SESSION.getBytes(US_ASCII), Long.MAX_VALUE, 2, 10,
            packetAllocator(pooled), reorderStorage(pooled))

        expect:
        arbiter.accept(A, packet(Long.MIN_VALUE, "second"), 0).empty
        describe(arbiter.accept(B, packet(Long.MAX_VALUE, "first"), 1)) ==
            [[Long.MAX_VALUE, B, "first"], [Long.MIN_VALUE, A, "second"]]
        arbiter.nextExpectedSequence() == Long.MIN_VALUE + 1

        where:
        pooled << [false, true]
    }

    def "synchronous delivery borrows the caller's buffer for every in-order message"() {
        given:
        def arbiter = newArbiter(true)
        def datagram = packet(1000, "AB", "C")
        def delivered = []
        RawMessageSink sink = { seq, leg, bytes, offset, length ->
            delivered << [seq, leg, bytes.is(datagram), offset, length, bytes.get(offset)]
        }

        when:
        arbiter.accept(A, datagram, 0, sink)

        then:
        delivered == [[1000L, A, true, 22, 2, 65], [1001L, A, true, 26, 1, 67]]
        datagram.position() == 0
    }

    def "pooled reordering preserves bytes and reuses the same payload storage after delivery"() {
        given:
        def arbiter = newArbiter(true, 1)
        def first = packet(1001, "B")
        def second = packet(1003, "D")
        def delivered = []
        // Save identity hashes, not references to buffers that are only borrowed.
        RawMessageSink sink = { seq, leg, bytes, offset, length ->
            if (leg == B) {
                assert !bytes.is(first) && !bytes.is(second)
            }
            delivered << [seq, leg, bytes.get(offset), System.identityHashCode(bytes)]
        }

        when:
        arbiter.accept(B, first, 0, sink)
        first.put(22, (byte) 0)
        arbiter.accept(A, packet(1000, "A"), 1, sink)
        arbiter.accept(B, second, 2, sink)
        second.put(22, (byte) 0)
        arbiter.accept(A, packet(1002, "C"), 3, sink)

        then:
        delivered.collect { it.take(3) } == [[1000L, A, 65], [1001L, B, 66], [1002L, A, 67], [1003L, B, 68]]
        delivered[1][3] == delivered[3][3]
        arbiter.stale().empty
    }

    def "a sink failing on sequence #failsAt stops delivery without retrying the failing message"() {
        given:
        def arbiter = newArbiter(true, 1)
        def failure = new IllegalStateException("sink failed")
        def attempted = []
        RawMessageSink sink = { seq, leg, bytes, offset, length ->
            attempted << seq
            if (seq == failsAt) {
                throw failure
            }
        }
        arbiter.accept(B, packet(1001, "B"), 0, sink)

        when:
        arbiter.accept(A, packet(1000, "A"), 1, sink)

        then:
        def actual = thrown(IllegalStateException)
        actual.is(failure)
        attempted == (1000L..failsAt).toList()
        arbiter.nextExpectedSequence() == failsAt
        arbiter.stale().orElseThrow().reason() == SINK_FAILURE

        when:
        arbiter.accept(A, packet(1000, "A", "B", "C"), 2, sink)

        then:
        attempted == (1000L..failsAt).toList()

        where:
        failsAt << [1000L, 1001L]
    }

    private static MoldUdp64PacketAllocator packetAllocator(boolean pooled) {
        pooled ? MoldUdp64PacketAllocator.pooled(1) : MoldUdp64PacketAllocator.heap()
    }

    private static MoldUdp64Arbiter.ReorderStorage reorderStorage(boolean pooled) {
        pooled ? MoldUdp64Arbiter.ReorderStorage.POOLED : MoldUdp64Arbiter.ReorderStorage.HEAP
    }

    private static MoldUdp64Arbiter newArbiter(boolean pooled, int capacity = 8) {
        new MoldUdp64Arbiter(SESSION.getBytes(US_ASCII), 1000, capacity, 10, packetAllocator(pooled), reorderStorage(pooled))
    }

    private static List describe(List<MoldUdp64Arbiter.Message> messages) {
        messages.collect { [it.sequence(), it.leg(), US_ASCII.decode(it.payload()).toString()] }
    }

    private static ByteBuffer packet(long sequence, String... messages) {
        def datagram = header(SESSION, sequence, messages.length)
        messages.each { message ->
            byte[] bytes = message.getBytes(US_ASCII)
            datagram.putShort((short) bytes.length).put(bytes)
        }
        datagram.flip()
    }

    private static ByteBuffer header(String session, long sequence, int count) {
        ByteBuffer.allocate(128).put(session.getBytes(US_ASCII)).putLong(sequence).putShort((short) count)
    }
}
