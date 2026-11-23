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

    /** Captures an immutable quote for callers that do not reuse a buffer. May allocate. */
    TopOfBook topOfBook();

    /**
     * Overwrites both sides of caller-owned scratch storage on the book thread.
     * The default uses the immutable quote; packed implementations can fill it directly.
     */
    default void readTopOfBook(TopOfBookBuffer target) {
        TopOfBook quote = topOfBook();
        PriceLevel bid = quote.bid();
        PriceLevel ask = quote.ask();
        target.setBid(bid == null ? 0 : bid.priceUnits(), bid == null ? 0 : bid.shares());
        target.setAsk(ask == null ? 0 : ask.priceUnits(), ask == null ? 0 : ask.shares());
    }

    /** Returns zero if the reference is not resting on the book. */
    long remainingShares(long orderReference);

    /** An absent side is represented by null. */
    record TopOfBook(PriceLevel bid, PriceLevel ask) {}

    /** Aggregate remaining shares at one price on one side. */
    record PriceLevel(long priceUnits, long shares) {}

    /**
     * Thread-confined scratch space. Zero shares indicate an absent side; zero
     * price is valid. Only immutable quotes made from this buffer may be shared.
     */
    final class TopOfBookBuffer {
        private long bidPrice;
        private long bidShares;
        private long askPrice;
        private long askShares;

        public void setBid(long priceUnits, long shares) {
            bidPrice = priceUnits;
            bidShares = shares;
        }

        public void setAsk(long priceUnits, long shares) {
            askPrice = priceUnits;
            askShares = shares;
        }

        /** Reuses unchanged immutable quote values from the previous publication. */
        public TopOfBook snapshot(TopOfBook previous) {
            PriceLevel bid = snapshotLevel(previous.bid(), bidPrice, bidShares);
            PriceLevel ask = snapshotLevel(previous.ask(), askPrice, askShares);
            return bid == previous.bid() && ask == previous.ask() ? previous : new TopOfBook(bid, ask);
        }

        private static PriceLevel snapshotLevel(PriceLevel previous, long priceUnits, long shares) {
            if (shares == 0) {
                return null;
            }
            if (previous != null && previous.priceUnits() == priceUnits && previous.shares() == shares) {
                return previous;
            }
            return new PriceLevel(priceUnits, shares);
        }
    }
}
