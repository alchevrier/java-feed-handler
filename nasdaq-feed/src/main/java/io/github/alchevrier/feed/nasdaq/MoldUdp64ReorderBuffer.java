package io.github.alchevrier.feed.nasdaq;

import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

/**
 * Owns copies of messages that must outlive the caller's datagram. The first
 * copy for each sequence wins. The arbiter decides when to deliver; removal
 * transfers the slot to the arbiter until it releases it after the callback.
 */
interface MoldUdp64ReorderBuffer {
    /** Returns false only when a new sequence will not fit. */
    boolean offer(long sequence, Leg leg, ByteBuffer source, int offset, int length);

    Slot remove(long sequence);

    void release(Slot slot);

    void clear();

    /** Keeps one writable buffer and a reusable read-only delivery view. */
    final class Slot {
        private final ByteBuffer storage;
        final ByteBuffer payload;
        Leg leg;
        int length;

        Slot(int capacity) {
            storage = ByteBuffer.allocate(capacity);
            payload = storage.asReadOnlyBuffer();
        }

        void copy(Leg leg, ByteBuffer source, int offset, int length) {
            storage.put(0, source, offset, length);
            payload.clear().limit(length);
            this.leg = leg;
            this.length = length;
        }
    }
}

/** Reference storage uses a hash map and allocates each payload slot to fit on demand. */
final class HeapReorderBuffer implements MoldUdp64ReorderBuffer {
    private final Map<Long, Slot> pending = new HashMap<>();
    private final int capacity;

    HeapReorderBuffer(int capacity) {
        this.capacity = capacity;
    }

    @Override
    public boolean offer(long sequence, Leg leg, ByteBuffer source, int offset, int length) {
        if (pending.containsKey(sequence)) {
            return true;
        }
        if (pending.size() == capacity) {
            return false;
        }
        Slot slot = new Slot(length);
        slot.copy(leg, source, offset, length);
        pending.put(sequence, slot);
        return true;
    }

    @Override
    public Slot remove(long sequence) {
        return pending.remove(sequence);
    }

    @Override
    public void release(Slot slot) {}

    @Override
    public void clear() {
        pending.clear();
    }
}

/**
 * Reserves one maximum-size payload buffer per slot so any valid message can
 * wait behind a gap without allocating. Sorted primitive sequence keys avoid
 * boxing; insertion and removal shift index entries, not payload bytes.
 */
final class PooledReorderBuffer implements MoldUdp64ReorderBuffer {
    private static final int MAX_PAYLOAD_LENGTH = 0xffff;
    private final long[] sequences;
    private final Slot[] pending;
    private final Slot[] free;
    private int size;
    private int freeCount;

    PooledReorderBuffer(int capacity) {
        sequences = new long[capacity];
        pending = new Slot[capacity];
        free = new Slot[capacity];
        for (int i = 0; i < capacity; i++) {
            free[i] = new Slot(MAX_PAYLOAD_LENGTH);
        }
        freeCount = capacity;
    }

    @Override
    public boolean offer(long sequence, Leg leg, ByteBuffer source, int offset, int length) {
        int index = Arrays.binarySearch(sequences, 0, size, sequence);
        if (index >= 0) {
            return true;
        }
        if (freeCount == 0) {
            return false;
        }
        index = -index - 1;
        Slot slot = free[--freeCount];
        slot.copy(leg, source, offset, length);
        System.arraycopy(sequences, index, sequences, index + 1, size - index);
        System.arraycopy(pending, index, pending, index + 1, size - index);
        sequences[index] = sequence;
        pending[index] = slot;
        size++;
        return true;
    }

    @Override
    public Slot remove(long sequence) {
        // Only exact matches matter; the arbiter, not the index, decides delivery order.
        int index = Arrays.binarySearch(sequences, 0, size, sequence);
        if (index < 0) {
            return null;
        }
        Slot slot = pending[index];
        --size;
        System.arraycopy(sequences, index + 1, sequences, index, size - index);
        System.arraycopy(pending, index + 1, pending, index, size - index);
        pending[size] = null;
        return slot;
    }

    @Override
    public void release(Slot slot) {
        free[freeCount++] = slot;
    }

    @Override
    public void clear() {
        while (size > 0) {
            release(pending[--size]);
            pending[size] = null;
        }
    }
}
