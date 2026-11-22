package io.github.alchevrier.feed.nasdaq;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/**
 * Merges both legs for one configured channel and session by unsigned message
 * sequence, not packet sequence or leg priority. It admits the first valid
 * copy, holds later sequences behind a gap, and stops delivery permanently
 * if the gap times out, storage fills, or the sink fails.
 *
 * <p>Call from one owner thread. The synchronous sink borrows in-order bytes
 * directly from the input; only pending messages need owned reorder storage.
 * The collecting overload copies all emitted messages for callers that retain
 * them after the call.
 */
public final class MoldUdp64Arbiter {
    private final byte[] session;
    private final long firstExpectedSequence;
    private final long gapTimeoutNanos;
    private final MoldUdp64PacketAllocator packetAllocator;
    private final MoldUdp64ReorderBuffer pending;
    private long expectedSequence;
    private long highestAdvertisedNext;
    private boolean gapOpen;
    private long gapOpenedAt;
    private long malformedDatagrams;
    private long wrongSessionDatagrams;
    private Stale stale;
    private boolean accepting;

    public MoldUdp64Arbiter(byte[] session, long firstExpectedSequence, int reorderCapacity, long gapTimeoutNanos) {
        this(session, firstExpectedSequence, reorderCapacity, gapTimeoutNanos, MoldUdp64PacketAllocator.heap());
    }

    public MoldUdp64Arbiter(byte[] session, long firstExpectedSequence, int reorderCapacity, long gapTimeoutNanos,
                           MoldUdp64PacketAllocator packetAllocator) {
        this(session, firstExpectedSequence, reorderCapacity, gapTimeoutNanos, packetAllocator, ReorderStorage.HEAP);
    }

    public MoldUdp64Arbiter(byte[] session, long firstExpectedSequence, int reorderCapacity, long gapTimeoutNanos,
                           MoldUdp64PacketAllocator packetAllocator, ReorderStorage reorderStorage) {
        if (session.length != MoldUdp64Packet.SESSION_LENGTH || reorderCapacity <= 0 || gapTimeoutNanos <= 0) {
            throw new IllegalArgumentException("Require a 10-byte session, positive capacity, and positive timeout");
        }
        this.session = session.clone();
        this.firstExpectedSequence = firstExpectedSequence;
        this.expectedSequence = firstExpectedSequence;
        this.highestAdvertisedNext = firstExpectedSequence;
        this.gapTimeoutNanos = gapTimeoutNanos;
        this.packetAllocator = Objects.requireNonNull(packetAllocator, "packetAllocator");
        this.pending = switch (reorderStorage) {
            case HEAP -> new HeapReorderBuffer(reorderCapacity);
            case POOLED -> new PooledReorderBuffer(reorderCapacity);
        };
    }

    /** Collects independently owned messages. Use the sink overload to avoid these copies. */
    public List<Message> accept(Leg leg, ByteBuffer datagram, long nowNanos) {
        List<Message> emitted = new ArrayList<>();
        accept(leg, datagram, nowNanos, (sequence, sourceLeg, source, offset, length) ->
                emitted.add(new Message(sequence, sourceLeg, source, offset, length)));
        return List.copyOf(emitted);
    }

    /**
     * Delivers newly released messages synchronously in sequence order. The
     * caller may reuse the datagram after return. Use the same monotonic
     * nanosecond time source for this and {@link #checkTimeout(long)}.
     *
     * @throws IllegalStateException if the packet allocator is exhausted;
     *         the datagram has not been consumed
     */
    public void accept(Leg leg, ByteBuffer datagram, long nowNanos, RawMessageSink sink) {
        requireIdle();
        Objects.requireNonNull(leg, "leg");
        Objects.requireNonNull(sink, "sink");
        expireGap(nowNanos);
        if (stale != null) {
            return;
        }

        MoldUdp64Packet packet = packetAllocator.tryAcquire();
        if (packet == null) {
            throw new IllegalStateException("MoldUDP64 packet allocator exhausted");
        }
        accepting = true;
        try {
            switch (packet.decode(datagram, session)) {
                case VALID -> admit(leg, datagram, packet, nowNanos, sink);
                case MALFORMED -> malformedDatagrams++;
                case WRONG_SESSION -> wrongSessionDatagrams++;
            }
        } finally {
            accepting = false;
            packet.clear();
            packetAllocator.release(packet);
        }
    }

    private void admit(Leg leg, ByteBuffer datagram, MoldUdp64Packet packet, long nowNanos, RawMessageSink sink) {
        long nextSequence = packet.nextSequence();
        if (Long.compareUnsigned(nextSequence, highestAdvertisedNext) > 0) {
            highestAdvertisedNext = nextSequence;
        }

        int cursor = packet.firstMessageOffset();
        for (long sequence = packet.firstSequence(); sequence != nextSequence; sequence++) {
            int length = packet.messageLengthAt(cursor);
            cursor += Short.BYTES;
            int order = Long.compareUnsigned(sequence, expectedSequence);
            if (order == 0) {
                deliver(sink, leg, datagram, cursor, length);
                drainPending(sink);
            } else if (order > 0 && !pending.offer(sequence, leg, datagram, cursor, length)) {
                markStale(StaleReason.REORDER_CAPACITY);
                return;
            }
            cursor += length;
        }
        updateGap(nowNanos);
    }

    /** Call while idle as well as on arrival so a silent leg cannot hide a gap. */
    public Optional<Stale> checkTimeout(long nowNanos) {
        requireIdle();
        expireGap(nowNanos);
        return stale();
    }

    public Optional<Stale> stale() {
        return Optional.ofNullable(stale);
    }

    public long nextExpectedSequence() {
        return expectedSequence;
    }

    public long malformedDatagrams() {
        return malformedDatagrams;
    }

    public long wrongSessionDatagrams() {
        return wrongSessionDatagrams;
    }

    private void drainPending(RawMessageSink sink) {
        MoldUdp64ReorderBuffer.Slot next;
        while ((next = pending.remove(expectedSequence)) != null) {
            try {
                deliver(sink, next.leg, next.payload, 0, next.length);
            } finally {
                pending.release(next);
            }
        }
    }

    private void deliver(RawMessageSink sink, Leg leg, ByteBuffer source, int offset, int length) {
        boolean delivered = false;
        try {
            sink.onMessage(expectedSequence, leg, source, offset, length);
            expectedSequence++;
            delivered = true;
        } finally {
            if (!delivered) {
                markStale(StaleReason.SINK_FAILURE);
            }
        }
    }

    private void requireIdle() {
        if (accepting) {
            throw new IllegalStateException("Cannot reenter the arbiter from its message sink");
        }
    }

    private void updateGap(long nowNanos) {
        if (expectedSequence == highestAdvertisedNext) {
            gapOpen = false;
        } else if (!gapOpen) {
            gapOpenedAt = nowNanos;
            gapOpen = true;
        }
    }

    private void expireGap(long nowNanos) {
        if (gapOpen && nowNanos - gapOpenedAt >= gapTimeoutNanos) {
            markStale(StaleReason.GAP_TIMEOUT);
        }
    }

    private void markStale(StaleReason reason) {
        OptionalLong last = expectedSequence == firstExpectedSequence
                ? OptionalLong.empty() : OptionalLong.of(expectedSequence - 1);
        stale = new Stale(reason, expectedSequence, last);
        gapOpen = false;
        pending.clear();
    }

    public enum Leg { A, B }

    public enum StaleReason { GAP_TIMEOUT, REORDER_CAPACITY, SINK_FAILURE }

    public enum ReorderStorage { HEAP, POOLED }

    /**
     * Borrows the indicated byte range only for the duration of the callback.
     * Use absolute reads; do not modify or retain the buffer or reenter
     * admission or timeout checks. If delivery throws, the arbiter stops
     * rather than retrying an event the sink may already have acted on.
     */
    @FunctionalInterface
    public interface RawMessageSink {
        void onMessage(long sequence, Leg leg, ByteBuffer source, int offset, int length);
    }

    /** Terminal status at the next undelivered sequence; emission does not imply the book applied it. */
    public record Stale(StaleReason reason, long missingSequence, OptionalLong lastEmittedSequence) {}

    /** Owned payload from the collecting API, safe to retain after the input buffer is reused. */
    public static final class Message {
        private final long sequence;
        private final Leg leg;
        private final byte[] payload;

        private Message(long sequence, Leg leg, ByteBuffer source, int offset, int length) {
            this.sequence = sequence;
            this.leg = leg;
            this.payload = new byte[length];
            source.get(offset, payload, 0, length);
        }

        public long sequence() {
            return sequence;
        }

        public Leg leg() {
            return leg;
        }

        public ByteBuffer payload() {
            return ByteBuffer.wrap(payload).asReadOnlyBuffer();
        }
    }
}
