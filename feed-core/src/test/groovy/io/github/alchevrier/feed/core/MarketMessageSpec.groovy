package io.github.alchevrier.feed.core

import spock.lang.Specification

import static io.github.alchevrier.feed.core.MarketMessage.Side.BUY
import static io.github.alchevrier.feed.core.MarketMessage.Side.SELL

class MarketMessageSpec extends Specification {

    def "add retains #side, quantity and fixed-point price"() {
        when:
        MarketMessage.Add message = MarketMessage.add(reference, side, shares, priceUnits)

        then:
        message.orderReference() == reference
        message.side() == side
        message.shares() == shares
        message.priceUnits() == priceUnits

        where:
        reference      | side | shares         | priceUnits
        42L            | BUY  | 100L           | 100_000L
        Long.MIN_VALUE | SELL | 4_294_967_295L | 4_294_967_295L
    }

    def "execute reports the share delta and delete identifies the same order"() {
        when:
        MarketMessage.Execute execution = MarketMessage.execute(reference, executedShares)
        MarketMessage.Delete deletion = MarketMessage.delete(reference)

        then:
        execution.orderReference() == reference
        execution.executedShares() == executedShares
        deletion.orderReference() == reference

        where:
        reference | executedShares
        42L       | 40L
        -1L       | 4_294_967_295L
    }

    def "rejects #scenario"() {
        when:
        createMessage.call()

        then:
        thrown(expectedError)

        where:
        scenario                      | createMessage                                | expectedError
        "zero add shares"             | { MarketMessage.add(42L, BUY, 0L, 100_000L) } | IllegalArgumentException
        "negative add shares"         | { MarketMessage.add(42L, BUY, -1L, 100_000L) } | IllegalArgumentException
        "negative price"              | { MarketMessage.add(42L, BUY, 100L, -1L) }    | IllegalArgumentException
        "missing side"                | { MarketMessage.add(42L, null, 100L, 0L) }    | NullPointerException
        "zero executed shares"        | { MarketMessage.execute(42L, 0L) }            | IllegalArgumentException
        "negative executed shares"    | { MarketMessage.execute(42L, -1L) }           | IllegalArgumentException
    }

    def "a reusable slot and a heap message expose the same execute contract"() {
        given:
        def slot = new ExecutionSlot()
        slot.reference = 42L
        slot.delta = 40L
        def fresh = MarketMessage.execute(42L, 40L)

        expect:
        describeExecution(slot) == describeExecution(fresh)

        when:
        slot.reference = 77L
        slot.delta = 25L

        then:
        describeExecution(slot) == [77L, 25L]
        describeExecution(fresh) == [42L, 40L]
    }

    private static List<Long> describeExecution(MarketMessage.Execute message) {
        [message.orderReference(), message.executedShares()]
    }

    private static class ExecutionSlot implements MarketMessage.Execute {
        long reference
        long delta

        @Override
        long orderReference() { reference }

        @Override
        long executedShares() { delta }
    }
}
