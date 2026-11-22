package io.github.alchevrier.feed.nasdaq;

import java.util.ArrayDeque;
import java.util.IdentityHashMap;

/**
 * Chooses how packet descriptors are leased without changing framing or
 * sequencing. Heap mode creates descriptors on demand; pooled mode reuses a
 * fixed number and reports exhaustion instead of allocating a fallback.
 * Neither mode owns the datagram bytes. Use from one owner thread.
 */
public interface MoldUdp64PacketAllocator {
    /** Returns an exclusively leased descriptor, or null when no slot is available. */
    MoldUdp64Packet tryAcquire();

    /** Returns a descriptor acquired from this allocator exactly once per lease. */
    void release(MoldUdp64Packet packet);

    static MoldUdp64PacketAllocator heap() {
        return new MoldUdp64PacketAllocator() {
            @Override
            public MoldUdp64Packet tryAcquire() {
                return new MoldUdp64Packet();
            }

            @Override
            public void release(MoldUdp64Packet packet) {
            }
        };
    }

    static MoldUdp64PacketAllocator pooled(int capacity) {
        return new PacketPool(capacity);
    }
}

/**
 * Tracks leases by object identity so a foreign or duplicate release cannot
 * put an extra descriptor on the free stack. All descriptors and bookkeeping
 * entries are allocated at construction.
 */
final class PacketPool implements MoldUdp64PacketAllocator {
    private final ArrayDeque<MoldUdp64Packet> available;
    private final IdentityHashMap<MoldUdp64Packet, Boolean> leased;

    PacketPool(int capacity) {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Packet pool capacity must be positive");
        }
        available = new ArrayDeque<>(capacity);
        leased = new IdentityHashMap<>(capacity);
        for (int i = 0; i < capacity; i++) {
            var packet = new MoldUdp64Packet();
            available.addLast(packet);
            leased.put(packet, false);
        }
    }

    @Override
    public MoldUdp64Packet tryAcquire() {
        var packet = available.pollLast();
        if (packet != null) {
            leased.put(packet, true);
        }
        return packet;
    }

    @Override
    public void release(MoldUdp64Packet packet) {
        if (!leased.replace(packet, true, false)) {
            throw new IllegalArgumentException("Packet is not leased from this pool");
        }
        available.addLast(packet);
    }
}
