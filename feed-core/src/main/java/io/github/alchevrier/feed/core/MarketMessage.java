package io.github.alchevrier.feed.core;

import java.util.Objects;

/**
 * A decoded order event, independent of transport framing and feed sequence.
 * The caller routes it to the right instrument and delivers it in order.
 *
 * <p>The sealed root fixes the event kinds, while each kind permits both
 * heap records and reusable slots. Read-only access does not imply immutable
 * storage: consumers must copy any values they need beyond the handoff.
 */
public sealed interface MarketMessage
        permits MarketMessage.Add, MarketMessage.Execute, MarketMessage.Delete {

    /** Opaque 64-bit exchange identifier; zero and negative bit patterns are valid. */
    long orderReference();

    /** Creates a resting order with a positive share count, side, and display price. */
    non-sealed interface Add extends MarketMessage {
        Side side();

        long shares();

        /** Display price in units of 1/10,000. */
        long priceUnits();
    }

    /** Reduces an existing order by reference; no price or match number is needed. */
    non-sealed interface Execute extends MarketMessage {
        /** Shares executed by this event, not the quantity remaining. */
        long executedShares();
    }

    /** Removes all remaining shares of an existing order by reference. */
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
