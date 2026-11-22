package io.github.alchevrier.feed.book;

import io.github.alchevrier.feed.core.MarketMessage;
import io.github.alchevrier.feed.core.MarketMessage.Side;

abstract class AbstractOrderBook implements OrderBook {
    protected static final long ALL_SHARES = 0;

    @Override
    public final boolean apply(MarketMessage message) {
        long reference = message.orderReference();
        return switch (message) {
            case MarketMessage.Add add -> {
                Side side = add.side();
                long price = add.priceUnits();
                long shares = add.shares();
                if (side == null || price < 0 || shares <= 0) {
                    throw new IllegalArgumentException("Add requires a side, non-negative price, and positive shares");
                }
                yield add(reference, side, price, shares);
            }
            case MarketMessage.Execute execution -> {
                long shares = execution.executedShares();
                if (shares <= 0) {
                    throw new IllegalArgumentException("Executed shares must be positive");
                }
                yield remove(reference, shares);
            }
            case MarketMessage.Delete ignored -> remove(reference, ALL_SHARES);
        };
    }

    protected abstract boolean add(long reference, Side side, long price, long shares);

    protected abstract boolean remove(long reference, long shares);

    protected static long sharesToRemove(long reference, long requested, long remaining) {
        if (remaining == 0) {
            throw new IllegalArgumentException("Unknown order reference: " + reference);
        }
        if (requested > remaining) {
            throw new IllegalArgumentException("Execution exceeds remaining shares for order: " + reference);
        }
        return requested == ALL_SHARES ? remaining : requested;
    }
}
