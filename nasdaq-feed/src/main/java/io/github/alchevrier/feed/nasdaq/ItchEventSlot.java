package io.github.alchevrier.feed.nasdaq;

import io.github.alchevrier.feed.core.MarketMessage;
import io.github.alchevrier.feed.core.MarketMessage.Side;

/** Reusable decoded fields with read-only, kind-specific views. Only one view is live at a time. */
final class ItchEventSlot {
    private final MarketMessage.Add add = new AddView();
    private final MarketMessage.Execute execute = new ExecuteView();
    private final MarketMessage.Delete delete = new DeleteView();
    private long reference;
    private long shares;
    private long priceUnits;
    private Side side;

    MarketMessage.Add add(long reference, Side side, long shares, long priceUnits) {
        this.reference = reference;
        this.side = side;
        this.shares = shares;
        this.priceUnits = priceUnits;
        return add;
    }

    MarketMessage.Execute execute(long reference, long shares) {
        this.reference = reference;
        this.shares = shares;
        return execute;
    }

    MarketMessage.Delete delete(long reference) {
        this.reference = reference;
        return delete;
    }

    private final class AddView implements MarketMessage.Add {
        @Override public long orderReference() { return reference; }
        @Override public Side side() { return side; }
        @Override public long shares() { return shares; }
        @Override public long priceUnits() { return priceUnits; }
    }

    private final class ExecuteView implements MarketMessage.Execute {
        @Override public long orderReference() { return reference; }
        @Override public long executedShares() { return shares; }
    }

    private final class DeleteView implements MarketMessage.Delete {
        @Override public long orderReference() { return reference; }
    }
}
