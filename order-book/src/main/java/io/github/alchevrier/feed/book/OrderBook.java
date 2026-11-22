package io.github.alchevrier.feed.book;

import io.github.alchevrier.feed.core.MarketMessage;

/**
 * Reconstructs the displayed book for a single instrument from already
 * sequenced events. The exchange decides which order fills; the book applies
 * that update by reference rather than matching orders or tracking FIFO priority.
 *
 * <p>One thread owns the mutable state so individual orders and their price
 * totals can be updated together. Other threads see only immutable snapshots
 * published by the caller.
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

    /** Aggregate remaining shares at one price on one side. */
    record PriceLevel(long priceUnits, long shares) {}
}
