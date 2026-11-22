package io.github.alchevrier.feed.book;

import io.github.alchevrier.feed.core.MarketMessage;

/**
 * A single-instrument book for decoded order events. Access the mutable book
 * only from its owner thread; publish immutable snapshots to other threads.
 */
public interface OrderBook {
    /**
     * Applies an event without retaining it. Returns true if the best price or
     * size changed on either side. A rejected event leaves the book unchanged.
     */
    boolean apply(MarketMessage message);

    /** Captures the current best price and aggregate shares on each side. */
    TopOfBook topOfBook();

    /** Returns zero if the reference is not resting on the book. */
    long remainingShares(long orderReference);

    /** An absent side is represented by null. */
    record TopOfBook(PriceLevel bid, PriceLevel ask) {}

    record PriceLevel(long priceUnits, long shares) {}
}
