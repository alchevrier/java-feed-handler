package io.github.alchevrier.feed.book;

import io.github.alchevrier.feed.core.MarketMessage.Side;

/**
 * Preallocates order slots and best-first price levels so successful updates
 * need no per-event allocation. Orders live in stable slots until removed;
 * they store the price, not a level index that can change when arrays shift.
 *
 * <p>This avoids a fixed tick grid at the cost of moving a suffix when a price
 * level appears or disappears. Capacity is fixed; exhaustion rejects the add
 * without evicting an order. Reading into caller-owned quote storage also
 * avoids allocation; {@link #topOfBook()} remains an immutable convenience read.
 */
public final class PackedOrderBook extends AbstractOrderBook {
    private static final int MAX_ORDER_CAPACITY = 1 << 29;

    private final long[] orderPrices;
    private final long[] orderShares;
    private final boolean[] orderBids;
    private final int[] freeSlots;
    private int freeCount;
    private final OrderIndex index;
    private final Levels bids;
    private final Levels asks;

    /**
     * @param maxOrders maximum live orders across both sides, from 1 to 2^29
     * @param maxLevelsPerSide maximum occupied prices on each side, positive
     */
    public PackedOrderBook(int maxOrders, int maxLevelsPerSide) {
        if (maxOrders <= 0 || maxOrders > MAX_ORDER_CAPACITY || maxLevelsPerSide <= 0) {
            throw new IllegalArgumentException("Require 1 to 2^29 order slots and positive level capacity");
        }
        orderPrices = new long[maxOrders];
        orderShares = new long[maxOrders];
        orderBids = new boolean[maxOrders];
        freeSlots = new int[maxOrders];
        for (int i = 0; i < maxOrders; i++) {
            freeSlots[i] = maxOrders - 1 - i;
        }
        freeCount = maxOrders;
        index = new OrderIndex(maxOrders);
        bids = new Levels(maxLevelsPerSide, true);
        asks = new Levels(maxLevelsPerSide, false);
    }

    @Override
    protected boolean add(long reference, Side side, long price, long shares) {
        int bucket = index.find(reference);
        if (index.slot(bucket) >= 0) {
            throw new IllegalArgumentException("Duplicate order reference: " + reference);
        }
        if (freeCount == 0) {
            throw new IllegalStateException("Order capacity exhausted");
        }
        boolean bid = side == Side.BUY;
        Levels levels = bid ? bids : asks;
        int level = levels.add(price, shares);
        int slot = freeSlots[--freeCount];
        orderPrices[slot] = price;
        orderShares[slot] = shares;
        orderBids[slot] = bid;
        index.insert(bucket, reference, slot);
        return level == 0;
    }

    @Override
    protected boolean remove(long reference, long shares) {
        int bucket = index.find(reference);
        int slot = index.slot(bucket);
        long remaining = slot < 0 ? 0 : orderShares[slot];
        long removed = sharesToRemove(reference, shares, remaining);
        Levels levels = orderBids[slot] ? bids : asks;
        int level = levels.find(orderPrices[slot]);
        levels.subtract(level, removed);
        orderShares[slot] = remaining - removed;
        if (orderShares[slot] == 0) {
            index.remove(bucket);
            freeSlots[freeCount++] = slot;
        }
        return level == 0;
    }

    @Override
    public TopOfBook topOfBook() {
        return new TopOfBook(bids.best(), asks.best());
    }

    @Override
    public void readTopOfBook(TopOfBookBuffer target) {
        target.setBid(bids.prices[0], bids.size == 0 ? 0 : bids.quantities[0]);
        target.setAsk(asks.prices[0], asks.size == 0 ? 0 : asks.quantities[0]);
    }

    @Override
    public long remainingShares(long orderReference) {
        int slot = index.slot(index.find(orderReference));
        return slot < 0 ? 0 : orderShares[slot];
    }

    /**
     * One side's occupied prices and quantities in parallel arrays. Binary
     * search finds the price or its insertion point in the live prefix;
     * changing quantity alone never shifts entries.
     */
    private static final class Levels {
        private final long[] prices;
        private final long[] quantities;
        private final boolean descending;
        private int size;

        private Levels(int capacity, boolean descending) {
            prices = new long[capacity];
            quantities = new long[capacity];
            this.descending = descending;
        }

        private int find(long price) {
            int low = 0;
            int high = size;
            while (low < high) {
                int mid = (low + high) >>> 1;
                if (descending ? prices[mid] > price : prices[mid] < price) {
                    low = mid + 1;
                } else {
                    high = mid;
                }
            }
            return low;
        }

        private int add(long price, long shares) {
            int position = find(price);
            if (position < size && prices[position] == price) {
                quantities[position] = Math.addExact(quantities[position], shares);
                return position;
            }
            if (size == prices.length) {
                throw new IllegalStateException("Price-level capacity exhausted");
            }
            System.arraycopy(prices, position, prices, position + 1, size - position);
            System.arraycopy(quantities, position, quantities, position + 1, size - position);
            prices[position] = price;
            quantities[position] = shares;
            size++;
            return position;
        }

        private void subtract(int position, long shares) {
            quantities[position] -= shares;
            if (quantities[position] == 0) {
                --size;
                System.arraycopy(prices, position + 1, prices, position, size - position);
                System.arraycopy(quantities, position + 1, quantities, position, size - position);
            }
        }

        private PriceLevel best() {
            return size == 0 ? null : new PriceLevel(prices[0], quantities[0]);
        }
    }

    /**
     * Maps an exchange reference to a stable order slot using linear probing
     * in a primitive array. The hash chooses where to start; the full reference
     * distinguishes colliding keys. Lookup stops at the first empty bucket,
     * so deletion moves entries whose probe path crosses the new hole. Only
     * bucket entries move, never the orders they point to.
     */
    private static final class OrderIndex {
        private final long[] references;
        // A bucket holds slot + 1 so zero is empty and every long is a valid reference.
        private final int[] buckets;
        private final int mask;

        private OrderIndex(int maxOrders) {
            references = new long[maxOrders];
            // Power of two, with enough buckets to stay at or below half full.
            buckets = new int[Integer.highestOneBit(2 * maxOrders - 1) << 1];
            mask = buckets.length - 1;
        }

        private int find(long reference) {
            int bucket = home(reference);
            while (buckets[bucket] != 0 && references[slot(bucket)] != reference) {
                bucket = (bucket + 1) & mask;
            }
            return bucket;
        }

        private int slot(int bucket) {
            return buckets[bucket] - 1;
        }

        private void insert(int bucket, long reference, int slot) {
            references[slot] = reference;
            buckets[bucket] = slot + 1;
        }

        private void remove(int hole) {
            for (int scan = (hole + 1) & mask; buckets[scan] != 0; scan = (scan + 1) & mask) {
                int home = home(references[slot(scan)]);
                // Move an entry back if its probe would otherwise stop at the hole.
                if (((hole - home) & mask) < ((scan - home) & mask)) {
                    buckets[hole] = buckets[scan];
                    hole = scan;
                }
            }
            buckets[hole] = 0;
        }

        private int home(long reference) {
            long hash = reference ^ (reference >>> 33);
            hash *= 0xff51afd7ed558ccdL;
            hash ^= hash >>> 33;
            return (int) hash & mask;
        }
    }
}
