package io.github.alchevrier.feed.core;

import java.util.Objects;

/**
 * A decoded event for a single-instrument order book. Implementations may
 * expose reusable storage, so consumers must not retain the message after use.
 */
public sealed interface MarketMessage
        permits MarketMessage.Add, MarketMessage.Execute, MarketMessage.Delete {

    long orderReference();

    non-sealed interface Add extends MarketMessage {
        Side side();

        long shares();

        /** Display price in units of 1/10,000. */
        long priceUnits();
    }

    non-sealed interface Execute extends MarketMessage {
        /** Shares executed by this event, not the quantity remaining. */
        long executedShares();
    }

    non-sealed interface Delete extends MarketMessage {}

    enum Side {
        BUY, SELL
    }

    static Add add(long orderReference, Side side, long shares, long priceUnits) {
        return new HeapAdd(orderReference, side, shares, priceUnits);
    }

    static Execute execute(long orderReference, long executedShares) {
        return new HeapExecute(orderReference, executedShares);
    }

    static Delete delete(long orderReference) {
        return new HeapDelete(orderReference);
    }
}

record HeapAdd(long orderReference, MarketMessage.Side side, long shares, long priceUnits)
        implements MarketMessage.Add {

    HeapAdd {
        Objects.requireNonNull(side, "side");
        if (shares <= 0) {
            throw new IllegalArgumentException("shares must be positive");
        }
        if (priceUnits < 0) {
            throw new IllegalArgumentException("priceUnits must be non-negative");
        }
    }
}

record HeapExecute(long orderReference, long executedShares) implements MarketMessage.Execute {

    HeapExecute {
        if (executedShares <= 0) {
            throw new IllegalArgumentException("executedShares must be positive");
        }
    }
}

record HeapDelete(long orderReference) implements MarketMessage.Delete {}
