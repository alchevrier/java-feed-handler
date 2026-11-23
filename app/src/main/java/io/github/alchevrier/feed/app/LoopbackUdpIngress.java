package io.github.alchevrier.feed.app;

import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.ClosedChannelException;
import java.nio.channels.DatagramChannel;
import java.util.concurrent.ArrayBlockingQueue;

/**
 * One socket and bounded ready queue per leg. A receiver always has a private
 * working buffer, so it can count and discard a datagram even when every
 * transferable buffer on its leg is queued or borrowed. Socket threads
 * never wait for the arbiter to return a buffer.
 */
final class LoopbackUdpIngress implements AutoCloseable {
    private static final int BUFFER_BYTES = 65_535;
    private final Receiver a;
    private final Receiver b;

    LoopbackUdpIngress(int capacity) throws IOException {
        if (capacity <= 0) {
            throw new IllegalArgumentException("Ingress capacity must be positive");
        }
        a = new Receiver(Leg.A, capacity);
        try {
            b = new Receiver(Leg.B, capacity);
        } catch (IOException e) {
            try {
                a.close();
            } catch (IOException closeFailure) {
                e.addSuppressed(closeFailure);
            }
            throw e;
        }
        a.thread.start();
        b.thread.start();
    }

    InetSocketAddress address(Leg leg) {
        return receiver(leg).address;
    }

    long dropped(Leg leg) {
        return receiver(leg).dropped;
    }

    /** One arbiter thread polls at most one datagram per leg per call. */
    boolean poll(DatagramSink sink) throws IOException {
        a.checkFailure();
        b.checkFailure();
        boolean fromA = a.poll(sink);
        boolean fromB = b.poll(sink);
        return fromA || fromB;
    }

    @Override
    public void close() throws IOException {
        try (a; b) {
            // Close both receivers even if one close fails.
        }
    }

    private Receiver receiver(Leg leg) {
        return switch (leg) {
            case A -> a;
            case B -> b;
        };
    }

    @FunctionalInterface
    interface DatagramSink {
        /** The datagram may only be read during this call. */
        void accept(Leg leg, ByteBuffer datagram);
    }

    private static final class Receiver implements AutoCloseable {
        private final Leg leg;
        private final DatagramChannel channel;
        private final InetSocketAddress address;
        private final ArrayBlockingQueue<ByteBuffer> ready;
        private final ArrayBlockingQueue<ByteBuffer> free;
        private final Thread thread;
        private ByteBuffer working = ByteBuffer.allocateDirect(BUFFER_BYTES);
        private volatile boolean closing;
        private volatile IOException failure;
        private volatile long dropped;

        private Receiver(Leg leg, int capacity) throws IOException {
            this.leg = leg;
            ready = new ArrayBlockingQueue<>(capacity);
            free = new ArrayBlockingQueue<>(capacity);
            for (int i = 0; i < capacity; i++) {
                free.add(ByteBuffer.allocateDirect(BUFFER_BYTES));
            }
            thread = Thread.ofPlatform().name("udp-leg-" + leg).unstarted(this::receive);
            channel = DatagramChannel.open();
            try {
                channel.bind(new InetSocketAddress(InetAddress.getLoopbackAddress(), 0));
                address = (InetSocketAddress) channel.getLocalAddress();
            } catch (IOException e) {
                try {
                    channel.close();
                } catch (IOException closeFailure) {
                    e.addSuppressed(closeFailure);
                }
                throw e;
            }
        }

        private void receive() {
            try {
                while (!closing) {
                    working.clear();
                    channel.receive(working);
                    ByteBuffer replacement = free.poll();
                    if (replacement == null) {
                        dropped++;
                    } else {
                        ready.add(working.flip());
                        working = replacement;
                    }
                }
            } catch (ClosedChannelException e) {
                if (!closing) {
                    failure = e;
                }
            } catch (IOException e) {
                failure = e;
            }
        }

        private void checkFailure() throws IOException {
            IOException cause = failure;
            if (cause != null) {
                throw new IOException("UDP receiver failed on leg " + leg, cause);
            }
        }

        private boolean poll(DatagramSink sink) {
            ByteBuffer datagram = ready.poll();
            if (datagram == null) {
                return false;
            }
            try {
                sink.accept(leg, datagram);
                return true;
            } finally {
                free.add(datagram);
            }
        }

        @Override
        public void close() throws IOException {
            closing = true;
            channel.close();
            try {
                thread.join();
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new IOException("Interrupted joining UDP receiver on leg " + leg, e);
            }
        }
    }
}
