package io.github.alchevrier.feed.nasdaq;

import io.github.alchevrier.feed.core.MarketMessage;
import io.github.alchevrier.feed.core.MarketMessage.Side;

import java.util.Objects;
import java.util.function.Consumer;

/**
 * Receives decoded fields so the mapper need not choose how to store events.
 * A target can allocate an owned event, reuse a synchronous slot, or write
 * into a slot claimed from a handoff.
 */
public interface ItchEventTarget {
    void add(long reference, Side side, long shares, long priceUnits);

    void execute(long reference, long executedShares);

    void delete(long reference);

    /** Creates an immutable event for each update; the consumer may retain it. */
    static ItchEventTarget heap(Consumer<? super MarketMessage> consumer) {
        Objects.requireNonNull(consumer, "consumer");
        return new ItchEventTarget() {
            @Override
            public void add(long reference, Side side, long shares, long priceUnits) {
                consumer.accept(MarketMessage.add(reference, side, shares, priceUnits));
            }

            @Override
            public void execute(long reference, long executedShares) {
                consumer.accept(MarketMessage.execute(reference, executedShares));
            }

            @Override
            public void delete(long reference) {
                consumer.accept(MarketMessage.delete(reference));
            }
        };
    }

    /**
     * Reuses one slot with a view per event kind. Use from one thread; the consumer may
     * read an event only during its callback and must not reenter this target.
     */
    static ItchEventTarget reusable(Consumer<? super MarketMessage> consumer) {
        return new ReusableItchEventTarget(consumer);
    }
}

final class ReusableItchEventTarget implements ItchEventTarget {
    private final Consumer<? super MarketMessage> consumer;
    private final ItchEventSlot slot = new ItchEventSlot();
    private boolean delivering;

    ReusableItchEventTarget(Consumer<? super MarketMessage> consumer) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
    }

    @Override
    public void add(long reference, Side side, long shares, long priceUnits) {
        requireIdle();
        deliver(slot.add(reference, side, shares, priceUnits));
    }

    @Override
    public void execute(long reference, long executedShares) {
        requireIdle();
        deliver(slot.execute(reference, executedShares));
    }

    @Override
    public void delete(long reference) {
        requireIdle();
        deliver(slot.delete(reference));
    }

    private void requireIdle() {
        if (delivering) {
            throw new IllegalStateException("Cannot overwrite an ITCH event during delivery");
        }
    }

    private void deliver(MarketMessage event) {
        delivering = true;
        try {
            consumer.accept(event);
        } finally {
            delivering = false;
        }
    }
}
