package io.github.alchevrier.feed.nasdaq

import spock.lang.Specification

import java.nio.ByteBuffer

import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.A
import static java.nio.charset.StandardCharsets.US_ASCII

class MoldUdp64PacketAllocatorSpec extends Specification {
    def "rejects zero pool capacity"() {
        when:
        MoldUdp64PacketAllocator.pooled(0)

        then:
        thrown(IllegalArgumentException)
    }

    def "#model allocates according to its policy"() {
        given:
        def allocator = model == "heap" ? MoldUdp64PacketAllocator.heap() : MoldUdp64PacketAllocator.pooled(1)
        def first = allocator.tryAcquire()

        expect:
        first != null
        (allocator.tryAcquire() == null) == reuses

        when:
        allocator.release(first)
        def next = allocator.tryAcquire()

        then:
        next != null
        next.is(first) == reuses

        where:
        model    | reuses
        "heap"   | false
        "pooled" | true
    }

    def "exhaustion is explicit and leaves the datagram available for retry"() {
        given:
        byte[] session = "SESSION001".getBytes(US_ASCII)
        def allocator = MoldUdp64PacketAllocator.pooled(1)
        def borrowed = allocator.tryAcquire()
        def arbiter = new MoldUdp64Arbiter(session, 1000, 1, 10, allocator)
        def datagram = ByteBuffer.allocate(23)
            .put(session).putLong(1000L).putShort((short) 1).putShort((short) 1).put((byte) 65).flip()

        when:
        arbiter.acceptAndCopy(A, datagram, 0)

        then:
        thrown(IllegalStateException)
        arbiter.nextExpectedSequence() == 1000
        arbiter.stale().empty
        datagram.position() == 0

        when:
        allocator.release(borrowed)
        def messages = arbiter.acceptAndCopy(A, datagram, 1)

        then:
        messages*.sequence() == [1000L]
        allocator.tryAcquire().is(borrowed)
    }

    def "pool rejects foreign and duplicate releases without handing out the same slot twice"() {
        given:
        def pool = MoldUdp64PacketAllocator.pooled(1)
        def other = MoldUdp64PacketAllocator.pooled(1)
        def slot = pool.tryAcquire()

        when:
        other.release(slot)

        then:
        thrown(IllegalArgumentException)
        pool.tryAcquire() == null

        when:
        pool.release(slot)
        pool.release(slot)

        then:
        thrown(IllegalArgumentException)
        pool.tryAcquire().is(slot)
        pool.tryAcquire() == null
    }
}
