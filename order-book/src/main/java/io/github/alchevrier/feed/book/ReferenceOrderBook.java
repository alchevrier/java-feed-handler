package io.github.alchevrier.feed.book;

import io.github.alchevrier.feed.core.MarketMessage.Side;

import java.util.Comparator;
import java.util.HashMap;
import java.util.NavigableMap;
import java.util.TreeMap;

/** Orders indexed by reference and aggregated into a sorted map per side. */
public final class ReferenceOrderBook extends AbstractOrderBook {
    private final HashMap<Long, Order> orders = new HashMap<>();
    private final NavigableMap<Long, Long> bids = new TreeMap<>(Comparator.reverseOrder());
    private final NavigableMap<Long, Long> asks = new TreeMap<>();

    @Override
    protected boolean add(long reference, Side side, long price, long shares) {
        if (orders.containsKey(reference)) {
            throw new IllegalArgumentException("Duplicate order reference: " + reference);
        }
        var levels = levels(side);
        long total = Math.addExact(levels.getOrDefault(price, 0L), shares);
        levels.put(price, total);
        orders.put(reference, new Order(side, price, shares));
        return levels.firstKey() == price;
    }

    @Override
    protected boolean remove(long reference, long shares) {
        Order order = orders.get(reference);
        long removed = sharesToRemove(reference, shares, order == null ? 0 : order.shares);
        var levels = levels(order.side);
        boolean changed = levels.firstKey() == order.price;
        long total = levels.get(order.price) - removed;
        if (total == 0) {
            levels.remove(order.price);
        } else {
            levels.put(order.price, total);
        }
        order.shares -= removed;
        if (order.shares == 0) {
            orders.remove(reference);
        }
        return changed;
    }

    @Override
    public TopOfBook topOfBook() {
        return new TopOfBook(best(bids), best(asks));
    }

    @Override
    public long remainingShares(long orderReference) {
        Order order = orders.get(orderReference);
        return order == null ? 0 : order.shares;
    }

    private NavigableMap<Long, Long> levels(Side side) {
        return side == Side.BUY ? bids : asks;
    }

    private static PriceLevel best(NavigableMap<Long, Long> levels) {
        var entry = levels.firstEntry();
        return entry == null ? null : new PriceLevel(entry.getKey(), entry.getValue());
    }

    private static final class Order {
        private final Side side;
        private final long price;
        private long shares;

        private Order(Side side, long price, long shares) {
            this.side = side;
            this.price = price;
            this.shares = shares;
        }
    }
}
