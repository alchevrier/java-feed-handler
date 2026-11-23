package io.github.alchevrier.feed.nasdaq;

import io.github.alchevrier.feed.core.MarketMessage;
import io.github.alchevrier.feed.core.MarketMessage.Side;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Connects MoldUDP64 admission and ITCH decoding to a book callback across a
 * bounded, preallocated single-producer, single-consumer ring. One producer
 * owns the arbiter and calls {@link #accept} and {@link #checkTimeout}; one
 * consumer calls {@link #tryConsume}. The caller supplies both threads.
 *
 * <p>Each volatile cursor has one writer. Publishing makes the filled slot
 * visible to the consumer; releasing makes it writable again. Neither
 * cursor uses wire sequence numbers because skipped messages do not occupy
 * slots. The first terminal failure is published independently of the ring.
 * A callback already claimed when the run becomes stale may finish, but no
 * more slots are consumed once staleness is observed.
 */
public final class ItchBookHandoff {
    private final MoldUdp64Arbiter arbiter;
    private final ItchMapper mapper;
    private final Entry[] entries;
    private final int mask;
    private final MoldUdp64Arbiter.RawMessageSink sink = this::decode;
    private final ItchEventTarget target = new RingTarget();
    private final AtomicReference<Stale> failure = new AtomicReference<>();
    private volatile long published;
    private volatile long released;
    private long decodingSequence;
    private boolean consuming;

    /** Takes exclusive ownership of a fresh arbiter. Capacity must be a positive power of two. */
    public ItchBookHandoff(MoldUdp64Arbiter arbiter, ItchMapper mapper, int capacity) {
        if (capacity <= 0 || Integer.bitCount(capacity) != 1) {
            throw new IllegalArgumentException("Handoff capacity must be a positive power of two");
        }
        this.arbiter = Objects.requireNonNull(arbiter, "arbiter");
        this.mapper = Objects.requireNonNull(mapper, "mapper");
        entries = new Entry[capacity];
        for (int i = 0; i < capacity; i++) {
            entries[i] = new Entry();
        }
        mask = capacity - 1;
    }

    /**
     * Producer only. Borrows the datagram until return. On overflow, stops at
     * the unqueued sequence and reports it via {@link #stale}; decode errors
     * also set stale status and propagate to the caller.
     */
    public void accept(MoldUdp64Arbiter.Leg leg, ByteBuffer datagram, long nowNanos) {
        if (failure.get() != null) {
            return;
        }
        try {
            arbiter.acceptAndForward(leg, datagram, nowNanos, sink);
        } catch (StopDelivery ignored) {
            // Stale status is already visible. Throwing inside the sink prevented sequence advancement.
        } finally {
            if (failure.get() == null) {
                recordFeedFailure(arbiter.stale().orElse(null));
            }
        }
    }

    /** Producer only. Check during idle periods using the same clock as {@link #accept}. */
    public void checkTimeout(long nowNanos) {
        if (failure.get() == null) {
            recordFeedFailure(arbiter.checkTimeout(nowNanos).orElse(null));
        }
    }

    /**
     * Consumer only. Borrows one queued event during the callback; returns
     * false if empty or stale. Do not retain the event or reenter this
     * method. If the callback throws, the run becomes stale and the
     * exception propagates.
     */
    public boolean tryConsume(BookEventConsumer consumer) {
        Objects.requireNonNull(consumer, "consumer");
        if (consuming) {
            throw new IllegalStateException("Cannot reenter the book handoff consumer");
        }
        long cursor = released;
        if (cursor == published || failure.get() != null) {
            return false;
        }
        Entry entry = entries[(int) cursor & mask];
        consuming = true;
        boolean applied = false;
        try {
            consumer.onEvent(entry.sequence, entry.event);
            applied = true;
            return true;
        } finally {
            if (!applied) {
                stop(StaleReason.BOOK_FAILURE, entry.sequence);
            }
            consuming = false;
            released = cursor + 1;
        }
    }

    /** Safe to read from either thread. An empty ring does not imply a healthy run. */
    public Optional<Stale> stale() {
        return Optional.ofNullable(failure.get());
    }

    private void decode(long sequence, MoldUdp64Arbiter.Leg leg, ByteBuffer source, int offset, int length) {
        if (failure.get() != null) {
            throw new StopDelivery();
        }
        decodingSequence = sequence;
        mapper.decode(source, offset, length, target);
    }

    private Entry claim() {
        if (published - released == entries.length) {
            stop(StaleReason.QUEUE_FULL, decodingSequence);
            throw new StopDelivery();
        }
        return entries[(int) published & mask];
    }

    private void publish(Entry entry, MarketMessage event) {
        entry.sequence = decodingSequence;
        entry.event = event;
        published++;
    }

    private void recordFeedFailure(MoldUdp64Arbiter.Stale stale) {
        if (stale == null) {
            return;
        }
        StaleReason reason = switch (stale.reason()) {
            case GAP_TIMEOUT -> StaleReason.GAP_TIMEOUT;
            case REORDER_CAPACITY -> StaleReason.REORDER_CAPACITY;
            case SINK_FAILURE -> StaleReason.DECODE_FAILURE;
        };
        stop(reason, stale.missingSequence());
    }

    private void stop(StaleReason reason, long sequence) {
        failure.compareAndSet(null, new Stale(reason, sequence));
    }

    @FunctionalInterface
    public interface BookEventConsumer {
        void onEvent(long sequence, MarketMessage event);
    }

    public enum StaleReason { QUEUE_FULL, GAP_TIMEOUT, REORDER_CAPACITY, DECODE_FAILURE, BOOK_FAILURE }

    /** Sequence at the failure boundary; previously published updates may still be unapplied. */
    public record Stale(StaleReason reason, long sequence) {}

    private static final class Entry {
        private final ItchEventSlot slot = new ItchEventSlot();
        private long sequence;
        private MarketMessage event;
    }

    private final class RingTarget implements ItchEventTarget {
        @Override
        public void add(long reference, Side side, long shares, long priceUnits) {
            Entry entry = claim();
            publish(entry, entry.slot.add(reference, side, shares, priceUnits));
        }

        @Override
        public void execute(long reference, long executedShares) {
            Entry entry = claim();
            publish(entry, entry.slot.execute(reference, executedShares));
        }

        @Override
        public void delete(long reference) {
            Entry entry = claim();
            publish(entry, entry.slot.delete(reference));
        }
    }

    private static final class StopDelivery extends RuntimeException {}
}
