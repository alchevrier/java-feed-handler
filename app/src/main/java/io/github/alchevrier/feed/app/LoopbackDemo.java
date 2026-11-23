package io.github.alchevrier.feed.app;

import io.github.alchevrier.feed.book.OrderBook;
import io.github.alchevrier.feed.book.PackedOrderBook;
import io.github.alchevrier.feed.nasdaq.ItchBookHandoff;
import io.github.alchevrier.feed.nasdaq.ItchMapper;
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter;
import io.github.alchevrier.feed.nasdaq.MoldUdp64PacketAllocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.A;
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.B;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.SECONDS;

/** Runs two finite synthetic sessions over loopback. Never connects to an exchange. */
public final class LoopbackDemo {
    private static final byte[] SESSION = "SESSION001".getBytes(US_ASCII);
    private static final int LOCATE = 7;
    private static final long FIRST_SEQUENCE = 1000;
    private static final long IDLE_PARK_NANOS = Duration.ofMillis(1).toNanos();

    private LoopbackDemo() {}

    public static void main(String[] args) throws Exception {
        print("Repaired gap", run(true));
        print("Unrepaired gap", run(false));
    }

    static Result run(boolean repair) throws Exception {
        long gapTimeout = (repair ? Duration.ofSeconds(2) : Duration.ofMillis(100)).toNanos();
        var arbiter = new MoldUdp64Arbiter(SESSION, FIRST_SEQUENCE, 4, gapTimeout,
                MoldUdp64PacketAllocator.pooled(1), MoldUdp64Arbiter.ReorderStorage.POOLED);
        var handoff = new ItchBookHandoff(arbiter, new ItchMapper(LOCATE), 4);
        var running = new AtomicBoolean(true);
        var sawLaterDatagram = new AtomicBoolean();
        var updates = new ArrayBlockingQueue<Update>(3);
        try (var ingress = new LoopbackUdpIngress(4);
             var sender = DatagramChannel.open();
             var workers = Executors.newFixedThreadPool(2, Thread.ofPlatform().name("loopback-worker-", 0).factory())) {
            Future<?> admission = workers.submit(() -> {
                LoopbackUdpIngress.DatagramSink sink = (leg, datagram) -> {
                    handoff.accept(leg, datagram, System.nanoTime());
                    if (leg == B) {
                        sawLaterDatagram.set(true);
                    }
                };
                while (running.get() && !Thread.currentThread().isInterrupted() && handoff.stale().isEmpty()) {
                    if (!ingress.poll(sink)) {
                        handoff.checkTimeout(System.nanoTime());
                        LockSupport.parkNanos(IDLE_PARK_NANOS);
                    }
                }
                return null;
            });
            Future<?> writing = workers.submit(() -> {
                OrderBook book = new PackedOrderBook(4, 4);
                ItchBookHandoff.BookEventConsumer apply = (sequence, event) -> {
                    book.apply(event);
                    updates.add(new Update(sequence, book.topOfBook().bid()));
                };
                while (running.get() && !Thread.currentThread().isInterrupted() && handoff.stale().isEmpty()) {
                    if (!handoff.tryConsume(apply)) {
                        LockSupport.parkNanos(IDLE_PARK_NANOS);
                    }
                }
            });
            try {
                send(sender, ingress.address(A), FIRST_SEQUENCE, add());
                await(() -> updates.size() == 1, "initial add", admission, writing);

                send(sender, ingress.address(B), FIRST_SEQUENCE + 2, orderMessage('D', 19));
                await(sawLaterDatagram::get, "out-of-order delete", admission, writing);
                if (repair) {
                    send(sender, ingress.address(A), FIRST_SEQUENCE + 1, orderMessage('E', 31).putInt(19, 40));
                    await(() -> updates.size() == 3 || handoff.stale().isPresent(), "gap repair", admission, writing);
                    if (handoff.stale().isPresent()) {
                        throw new IllegalStateException("Repair failed: " + handoff.stale().orElseThrow());
                    }
                } else {
                    await(() -> handoff.stale().isPresent(), "gap timeout", admission, writing);
                }
            } finally {
                running.set(false);
            }
            admission.get(5, SECONDS);
            writing.get(5, SECONDS);
            return new Result(List.copyOf(updates), handoff.stale().orElse(null));
        }
    }

    private static void await(BooleanSupplier done, String phase, Future<?> admission, Future<?> writing)
            throws Exception {
        long start = System.nanoTime();
        while (!done.getAsBoolean()) {
            if (admission.isDone()) {
                admission.get();
            }
            if (writing.isDone()) {
                writing.get();
            }
            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted waiting for " + phase);
            }
            if (System.nanoTime() - start >= SECONDS.toNanos(5)) {
                throw new TimeoutException("Loopback demo did not observe " + phase);
            }
            LockSupport.parkNanos(IDLE_PARK_NANOS);
        }
    }

    private static void send(DatagramChannel sender, InetSocketAddress address, long sequence,
                             ByteBuffer message) throws IOException {
        ByteBuffer datagram = ByteBuffer.allocate(20 + Short.BYTES + message.remaining())
                .put(SESSION).putLong(sequence).putShort((short) 1)
                .putShort((short) message.remaining()).put(message).flip();
        int size = datagram.remaining();
        if (sender.send(datagram, address) != size) {
            throw new IOException("Could not send demo datagram for sequence " + sequence);
        }
    }

    private static ByteBuffer add() {
        return orderMessage('A', 36).put(19, (byte) 'B').putInt(20, 100).putInt(32, 100_000);
    }

    private static ByteBuffer orderMessage(char type, int length) {
        return ByteBuffer.allocate(length).put(0, (byte) type).putShort(1, (short) LOCATE).putLong(11, 42L);
    }

    private static void print(String label, Result result) {
        System.out.println(label + ":");
        for (Update update : result.updates()) {
            System.out.printf("  applied %d: bid=%s%n", update.sequence(), update.bid());
        }
        if (result.stale() != null) {
            System.out.printf("  STALE %s at sequence %d%n", result.stale().reason(), result.stale().sequence());
        }
    }

    record Update(long sequence, OrderBook.PriceLevel bid) {}

    record Result(List<Update> updates, ItchBookHandoff.Stale stale) {}
}
