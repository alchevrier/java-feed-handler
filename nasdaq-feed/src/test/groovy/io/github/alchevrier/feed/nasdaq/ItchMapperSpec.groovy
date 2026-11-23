package io.github.alchevrier.feed.nasdaq

import io.github.alchevrier.feed.core.MarketMessage
import spock.lang.Specification

import java.nio.ByteBuffer
import java.nio.ByteOrder

import static io.github.alchevrier.feed.core.MarketMessage.Side.BUY
import static io.github.alchevrier.feed.core.MarketMessage.Side.SELL

class ItchMapperSpec extends Specification {
    private static final int LOCATE = 0xfffe
    private static final long REFERENCE = 0xfedcba9876543210L

    def "decodes A/E/D from a borrowed range in #order buffer order"() {
        given:
        def mapper = new ItchMapper(LOCATE)
        List<MarketMessage> events = []
        def target = ItchEventTarget.heap(events::add)
        def add = wire('A', 36)
            .put(19, (byte) 'S').putInt(20, -1).putInt(32, (int) 0x89abcdefL)
        def execute = wire('E', 31).putInt(19, Integer.MIN_VALUE).putLong(23, 123L)
        def delete = wire('D', 19)

        when:
        [add, execute, delete].each { payload ->
            def input = ByteBuffer.allocateDirect(payload.limit() + 8)
                .position(5).put(payload).flip().position(3).asReadOnlyBuffer().order(order)
            mapper.decode(input, 5, input.limit() - 5, target)
            assert input.position() == 3
            assert input.limit() == payload.capacity() + 5
            assert input.order() == order
        }

        then:
        events == [
            MarketMessage.add(REFERENCE, SELL, 0xffff_ffffL, 0x89ab_cdefL),
            MarketMessage.execute(REFERENCE, 0x8000_0000L),
            MarketMessage.delete(REFERENCE)
        ]

        where:
        order << [ByteOrder.BIG_ENDIAN, ByteOrder.LITTLE_ENDIAN]
    }

    def "reusable target emits stable typed slots with newly decoded values"() {
        given:
        def mapper = new ItchMapper(LOCATE)
        def observed = []
        def target = ItchEventTarget.reusable({ MarketMessage event ->
            observed << [System.identityHashCode(event), switchValues(event)]
        })

        when:
        [1L, 2L].each { reference ->
            mapper.decode(wire('A', 36).putLong(11, reference)
                .put(19, (byte) 'B').putInt(20, (int) reference).putInt(32, 100), 0, 36, target)
            mapper.decode(wire('E', 31).putLong(11, reference).putInt(19, (int) reference), 0, 31, target)
            mapper.decode(wire('D', 19).putLong(11, reference), 0, 19, target)
        }

        then:
        observed*.last() == [
            [1L, BUY, 1L, 100L], [1L, 1L], [1L],
            [2L, BUY, 2L, 100L], [2L, 2L], [2L]
        ]
        observed.take(3)*.first() == observed.drop(3)*.first()
    }

    def "reusable target rejects reentrant writes and releases its slot when the consumer throws"() {
        given:
        def mapper = new ItchMapper(LOCATE)
        def observed = []
        ItchEventTarget target
        target = ItchEventTarget.reusable({ MarketMessage event ->
            observed << event.orderReference()
            if (event.orderReference() == 1L) {
                mapper.decode(wire('D', 19).putLong(11, 2L), 0, 19, target)
            }
        })

        when:
        mapper.decode(wire('D', 19).putLong(11, 1L), 0, 19, target)

        then:
        thrown(IllegalStateException)
        observed == [1L]

        when:
        mapper.decode(wire('D', 19).putLong(11, 3L), 0, 19, target)

        then:
        observed == [1L, 3L]
    }

    def "ignores recognized non-book type #type but requires its exact wire length"() {
        given:
        def mapper = new ItchMapper(LOCATE)
        def target = ItchEventTarget.heap({ throw new AssertionError("Unexpected event") })
        def payload = wire(type, length)

        when:
        mapper.decode(payload, 0, length, target)

        then:
        noExceptionThrown()

        when:
        mapper.decode(payload, 0, length - 1, target)

        then:
        thrown(IllegalArgumentException)

        where:
        type | length
        'S'  | 12
        'R'  | 39
        'H'  | 25
        'Y'  | 20
        'L'  | 26
        'V'  | 35
        'W'  | 12
        'K'  | 28
        'J'  | 35
        'h'  | 21
        'P'  | 44
        'Q'  | 40
        'B'  | 19
        'I'  | 50
        'N'  | 20
        'O'  | 48
    }

    def "rejects unsupported mutation #type only for the configured stock locate"() {
        given:
        def mapper = new ItchMapper(LOCATE)
        def target = ItchEventTarget.heap({ throw new AssertionError("Unexpected event") })
        def payload = wire(type, length)

        when:
        mapper.decode(payload, 0, length, target)

        then:
        thrown(UnsupportedOperationException)

        when:
        payload.putShort(1, (short) 42)
        mapper.decode(payload, 0, length, target)

        then:
        noExceptionThrown()

        where:
        type | length
        'F'  | 40
        'C'  | 36
        'X'  | 23
        'U'  | 35
    }

    def "does not emit supported book events for another stock locate"() {
        given:
        def mapper = new ItchMapper(LOCATE)
        def target = ItchEventTarget.heap({ throw new AssertionError("Unexpected event") })
        def other = wire('D', 19).putShort(1, (short) 42)

        when:
        mapper.decode(other, 0, 19, target)

        then:
        noExceptionThrown()
    }

    def "rejects #scenario before writing any event"() {
        given:
        def mapper = new ItchMapper(LOCATE)
        def target = ItchEventTarget.heap({ throw new AssertionError("Unexpected event") })

        when:
        mapper.decode(payload, offset, length, target)

        then:
        thrown(IllegalArgumentException)

        where:
        scenario                        | payload                                 | offset            | length
        "empty payload"                 | ByteBuffer.allocate(0)                  | 0                 | 0
        "negative offset"               | wire('D', 19)                           | -1                | 19
        "offset before source position" | wire('D', 19).position(1)               | 0                 | 19
        "offset beyond the limit"       | wire('D', 19)                           | Integer.MAX_VALUE | 19
        "length beyond the limit"       | wire('D', 19)                           | 1                 | 19
        "negative length"               | wire('D', 19)                           | 0                 | -1
        "truncated add"                 | wire('A', 35)                           | 0                 | 35
        "oversized execute"             | wire('E', 32)                           | 0                 | 32
        "truncated foreign delete"      | wire('D', 18).putShort(1, (short) 42)  | 0                 | 18
        "unknown type"                  | wire('?', 19).putShort(1, (short) 42)  | 0                 | 19
        "unknown add side"              | wire('A', 36).putInt(20, 1)             | 0                 | 36
        "zero add shares"               | wire('A', 36).put(19, (byte) 'B')      | 0                 | 36
        "zero executed shares"          | wire('E', 31)                           | 0                 | 31
    }

    def "rejects a stock locate outside the unsigned 16-bit range"() {
        when:
        new ItchMapper(locate)

        then:
        thrown(IllegalArgumentException)

        where:
        locate << [-1, 0x10000]
    }

    private static List switchValues(MarketMessage event) {
        if (event instanceof MarketMessage.Add) {
            return [event.orderReference(), event.side(), event.shares(), event.priceUnits()]
        }
        if (event instanceof MarketMessage.Execute) {
            return [event.orderReference(), event.executedShares()]
        }
        [event.orderReference()]
    }

    private static ByteBuffer wire(String type, int length) {
        def payload = ByteBuffer.allocate(length)
            .put(0, (byte) type.charAt(0))
            .putShort(1, (short) LOCATE)
        if (length >= 19) {
            payload.putLong(11, REFERENCE)
        }
        payload
    }
}
