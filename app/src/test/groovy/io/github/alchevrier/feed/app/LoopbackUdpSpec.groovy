package io.github.alchevrier.feed.app

import io.github.alchevrier.feed.book.OrderBook.PriceLevel
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg
import spock.lang.Specification
import spock.util.concurrent.PollingConditions

import java.nio.ByteBuffer
import java.nio.channels.DatagramChannel

import static io.github.alchevrier.feed.nasdaq.ItchBookHandoff.StaleReason.GAP_TIMEOUT
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.A
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.B

class LoopbackUdpSpec extends Specification {
    def "loopback #scenario applies only the contiguous prefix to the book"() {
        when:
        def result = LoopbackDemo.run(repair)

        then:
        result.updates()*.sequence() == sequences
        result.updates()*.bid() == bids
        result.stale()?.reason() == reason
        result.stale()?.sequence() == missing

        where:
        scenario        | repair | sequences             | bids                                                         | reason      | missing
        'repairs a gap' | true   | [1000L, 1001L, 1002L] | [new PriceLevel(100_000, 100), new PriceLevel(100_000, 60), null] | null        | null
        'times out'     | false  | [1000L]               | [new PriceLevel(100_000, 100)]                                | GAP_TIMEOUT | 1001L
    }

    def "a borrowed datagram survives a drop on its leg and the other leg remains usable"() {
        given:
        def ingress = new LoopbackUdpIngress(1)
        def sender = DatagramChannel.open()
        def conditions = new PollingConditions(timeout: 5)
        def delivered = []
        LoopbackUdpIngress.DatagramSink capture = { Leg leg, ByteBuffer datagram ->
            delivered << [leg, datagram.getInt()]
        }
        send(sender, ingress, A, 11)

        expect:
        conditions.eventually {
            assert ingress.poll { Leg leg, ByteBuffer datagram ->
                if (leg == A) {
                    assert datagram.remaining() == Integer.BYTES
                    assert datagram.getInt(0) == 11
                    send(sender, ingress, A, 22)
                    send(sender, ingress, B, 33)
                    conditions.eventually { assert ingress.dropped(A) == 1 }
                    assert datagram.getInt(0) == 11
                }
                capture.accept(leg, datagram)
            }
        }
        conditions.eventually { assert delivered.size() == 2 || ingress.poll(capture) }
        delivered == [[A, 11], [B, 33]]
        ingress.dropped(B) == 0

        when:
        send(sender, ingress, A, 44)

        then:
        conditions.eventually { assert ingress.poll(capture) }
        delivered == [[A, 11], [B, 33], [A, 44]]
        ingress.dropped(A) == 1

        cleanup:
        sender?.close()
        ingress?.close()
    }

    def "a failing arbiter callback returns its datagram buffer"() {
        given:
        def ingress = new LoopbackUdpIngress(1)
        def sender = DatagramChannel.open()
        def conditions = new PollingConditions(timeout: 5)
        send(sender, ingress, A, 11)

        expect:
        conditions.eventually {
            IllegalStateException failure = null
            try {
                ingress.poll { leg, datagram -> throw new IllegalStateException('sink failed') }
            } catch (IllegalStateException e) {
                failure = e
            }
            assert failure?.message == 'sink failed'
        }

        when:
        send(sender, ingress, A, 22)

        then:
        conditions.eventually {
            assert ingress.poll { Leg leg, ByteBuffer datagram ->
                assert leg == A
                assert datagram.getInt() == 22
            }
        }
        ingress.dropped(A) == 0

        cleanup:
        sender?.close()
        ingress?.close()
    }

    private static void send(DatagramChannel sender, LoopbackUdpIngress ingress, Leg leg, int value) {
        assert sender.send(ByteBuffer.allocate(Integer.BYTES).putInt(value).flip(),
                ingress.address(leg)) == Integer.BYTES
    }
}
