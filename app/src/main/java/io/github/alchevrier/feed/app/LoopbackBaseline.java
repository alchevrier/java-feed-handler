package io.github.alchevrier.feed.app;

import io.github.alchevrier.feed.book.OrderBook;
import io.github.alchevrier.feed.book.PackedOrderBook;
import io.github.alchevrier.feed.book.ReferenceOrderBook;
import io.github.alchevrier.feed.nasdaq.ItchBookHandoff;
import io.github.alchevrier.feed.nasdaq.ItchMapper;
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter;
import io.github.alchevrier.feed.nasdaq.MoldUdp64PacketAllocator;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.channels.DatagramChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.Executors;
import java.util.concurrent.locks.LockSupport;

import static io.github.alchevrier.feed.app.TopOfBookPublisher.Status.STALE;
import static io.github.alchevrier.feed.nasdaq.ItchBookHandoff.StaleReason.QUEUE_FULL;
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.A;
import static io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg.B;
import static java.nio.charset.StandardCharsets.US_ASCII;
import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static java.util.concurrent.TimeUnit.SECONDS;

/**
 * Open-loop synthetic sender with one timing sample per successful book
 * publication. Stores timings in primitive arrays on the sending and book
 * threads and summarizes them only after the workers have stopped.
 */
public final class LoopbackBaseline {
    private static final byte[] SESSION = "BASELINE01".getBytes(US_ASCII);
    private static final int LOCATE = 7;
    private static final int INGRESS_CAPACITY = 64;
    private static final int BOOK_CAPACITY = 256;
    private static final int REORDER_CAPACITY = 64;
    private static final long GAP_TIMEOUT = MILLISECONDS.toNanos(250);
    private static final long IDLE_PARK = MILLISECONDS.toNanos(1);

    private LoopbackBaseline() {}

    public static void main(String[] args) throws Exception {
        Config config = switch (args.length) {
            case 0 -> new Config(1024, 2, 5);
            case 3, 4 -> new Config(Integer.parseInt(args[0]), Integer.parseInt(args[1]), Integer.parseInt(args[2]));
            default -> throw new IllegalArgumentException(
                    "Expected: [updatesPerSecond warmupSeconds sampleSeconds [reference|packed|both]]");
        };
        String selection = args.length == 4 ? args[3] : "reference";
        List<BookLayout> layouts = switch (selection) {
            case "reference" -> List.of(BookLayout.REFERENCE);
            case "packed" -> List.of(BookLayout.PACKED);
            case "both" -> List.of(BookLayout.REFERENCE, BookLayout.PACKED);
            default -> throw new IllegalArgumentException("Unknown book layout: " + selection);
        };
        Path root = Files.createDirectories(Path.of("build", "reports", "loopback-baseline"));
        Path output = Files.createTempDirectory(root, "run-");
        System.out.printf("%d updates/s; warmup=%ds; measurement=%ds; two datagrams/update%n",
                config.rate(), config.warmupSeconds(), config.sampleSeconds());
        for (Load load : Load.values()) {
            for (BookLayout layout : load == Load.BURSTY ? layouts.reversed() : layouts) {
                Result result = run(load, config, layout);
                String file = (layout.name() + "-" + load.name()).toLowerCase(Locale.ROOT) + ".csv";
                result.writeCsv(output.resolve(file));
                print(layout, load, result);
            }
        }
        System.out.println("Per-event CSV: " + output.toAbsolutePath());
    }

    private static void print(BookLayout layout, Load load, Result result) {
        System.out.printf(Locale.ROOT,
                "%s %s %s: offered=%d applied=%d unapplied=%d applied_rate=%.1f/s "
                        + "ingress_drops(A/B)=%d/%d book_queue_full=%d%n",
                layout, load, result.outcome(), result.offered(), result.applied(), result.offered() - result.applied(),
                result.appliedRate(), result.droppedA(), result.droppedB(), result.bookQueueFull());
        System.out.printf("  ns: send-to-publication %s; deadline-to-publication %s%n",
                result.sendToPublish(), result.deadlineToPublish());
        result.latest().failure().ifPresent(failure ->
                System.out.printf("  stale_reason=%s sequence=%d%n", failure.reason(), failure.sequence()));
    }

    static Result run(Load load, Config config, BookLayout layout) throws Exception {
        var samples = new Samples(config);
        var publisher = new TopOfBookPublisher();
        var arbiter = new MoldUdp64Arbiter(SESSION, 1, REORDER_CAPACITY, GAP_TIMEOUT,
                MoldUdp64PacketAllocator.pooled(1), MoldUdp64Arbiter.ReorderStorage.POOLED);
        var handoff = new ItchBookHandoff(arbiter, new ItchMapper(LOCATE), BOOK_CAPACITY);
        var pipeline = new SnapshotBookPipeline(handoff, layout.createBook(), publisher, samples::published);
        long droppedA;
        long droppedB;
        try (var ingress = new LoopbackUdpIngress(INGRESS_CAPACITY)) {
            try (var sender = DatagramChannel.open();
                 var workers = Executors.newFixedThreadPool(2, Thread.ofPlatform().name("baseline-worker-", 0).factory())) {
                var admission = workers.submit(() -> {
                    LoopbackUdpIngress.DatagramSink sink =
                            (leg, datagram) -> pipeline.accept(leg, datagram, System.nanoTime());
                    while (!Thread.currentThread().isInterrupted() && publisher.latest().status() != STALE) {
                        if (!ingress.poll(sink)) {
                            pipeline.checkTimeout(System.nanoTime());
                            LockSupport.parkNanos(IDLE_PARK);
                        }
                    }
                    return null;
                });
                var book = workers.submit(() -> {
                    while (!Thread.currentThread().isInterrupted() && publisher.latest().status() != STALE) {
                        if (!pipeline.tryApplyNext()) {
                            LockSupport.parkNanos(IDLE_PARK);
                        }
                    }
                    return null;
                });
                try {
                    drive(sender, ingress.address(A), ingress.address(B), load, config, samples);
                    awaitPublication(publisher, samples.total());
                } finally {
                    workers.shutdownNow();
                }
                admission.get();
                book.get();
            }
            droppedA = ingress.dropped(A);
            droppedB = ingress.dropped(B);
        }
        return new Result(samples, publisher.latest(), droppedA, droppedB);
    }

    private static void drive(DatagramChannel sender, InetSocketAddress a, InetSocketAddress b,
                              Load load, Config config, Samples samples) throws IOException, InterruptedException {
        ByteBuffer datagram = ByteBuffer.allocateDirect(58);
        long start = System.nanoTime();
        int warmup = config.warmupEvents();
        long measurementStart = start + SECONDS.toNanos(config.warmupSeconds());
        for (int i = 0; i < samples.total(); i++) {
            long deadline = i < warmup ? start + load.offsetNanos(i, config.rate())
                    : measurementStart + load.offsetNanos(i - warmup, config.rate());
            encodeUpdate(datagram, i);
            InetSocketAddress first = (i & 1) == 0 ? a : b;
            InetSocketAddress second = (i & 1) == 0 ? b : a;
            waitUntil(deadline);
            samples.deadlines[i] = deadline;
            samples.sent[i] = System.nanoTime();
            sendBoth(sender, datagram, first, second);
        }
        samples.measurementEnd = measurementStart + SECONDS.toNanos(config.sampleSeconds());
        waitUntil(samples.measurementEnd);
        datagram.clear().put(SESSION).putLong(samples.total() + 1L).putShort((short) 0).flip();
        sendBoth(sender, datagram, a, b);
    }

    private static void encodeUpdate(ByteBuffer datagram, int index) {
        int cycle = index / 3;
        int phase = index % 3;
        int payloadLength = switch (phase) {
            case 0 -> 36;
            case 1 -> 31;
            default -> 19;
        };
        datagram.clear().put(SESSION).putLong(index + 1L).putShort((short) 1)
                .putShort((short) payloadLength);
        int payload = datagram.position();
        datagram.limit(payload + payloadLength);
        datagram.put(payload, switch (phase) {
            case 0 -> (byte) 'A';
            case 1 -> (byte) 'E';
            default -> (byte) 'D';
        }).putShort(payload + 1, (short) LOCATE).putLong(payload + 11, cycle + 1L);
        if (phase == 0) {
            datagram.put(payload + 19, (byte) ((cycle & 1) == 0 ? 'B' : 'S'))
                    .putInt(payload + 20, 100).putInt(payload + 32, 100_000 + cycle % 32);
        } else if (phase == 1) {
            datagram.putInt(payload + 19, 40);
        }
        datagram.position(0);
    }

    private static void sendBoth(DatagramChannel channel, ByteBuffer datagram,
                                 InetSocketAddress first, InetSocketAddress second) throws IOException {
        int bytes = datagram.remaining();
        if (channel.send(datagram, first) != bytes || channel.send(datagram.rewind(), second) != bytes) {
            throw new IOException("Incomplete synthetic UDP send");
        }
    }

    private static void waitUntil(long deadline) throws InterruptedException {
        long remaining;
        while ((remaining = deadline - System.nanoTime()) > 0) {
            if (Thread.interrupted()) {
                throw new InterruptedException("Interrupted while pacing loopback traffic");
            }
            if (remaining > 50_000) {
                LockSupport.parkNanos(remaining - 50_000);
            } else {
                Thread.onSpinWait();
            }
        }
    }

    private static void awaitPublication(TopOfBookPublisher publisher, int expected) throws InterruptedException {
        long deadline = System.nanoTime() + SECONDS.toNanos(2);
        while (deadline - System.nanoTime() > 0) {
            var snapshot = publisher.latest();
            if (snapshot.status() == STALE || snapshot.lastAppliedSequence().orElse(0) == expected) {
                return;
            }
            waitUntil(System.nanoTime() + IDLE_PARK);
        }
    }

    enum BookLayout {
        REFERENCE, PACKED;

        private OrderBook createBook() {
            return switch (this) {
                case REFERENCE -> new ReferenceOrderBook();
                case PACKED -> new PackedOrderBook(1, 1);
            };
        }
    }

    enum Load {
        STEADY(1), BURSTY(16);

        private final int batch;

        Load(int batch) {
            this.batch = batch;
        }

        long offsetNanos(int index, int rate) {
            return SECONDS.toNanos(1) * (index - index % batch) / rate;
        }
    }

    record Config(int rate, int warmupSeconds, int sampleSeconds) {
        Config {
            if (rate <= 0 || warmupSeconds < 0 || sampleSeconds <= 0
                    || rate * ((long) warmupSeconds + sampleSeconds) > 1_000_000) {
                throw new IllegalArgumentException("Require positive rate and measurement, nonnegative warmup, "
                        + "and at most one million updates");
            }
        }

        int warmupEvents() {
            return rate * warmupSeconds;
        }

        int sampleEvents() {
            return rate * sampleSeconds;
        }
    }

    private static final class Samples {
        private final int warmup;
        private final long[] deadlines;
        private final long[] sent;
        private final long[] published;
        private int publishedCount;
        private long measurementEnd;

        Samples(Config config) {
            warmup = config.warmupEvents();
            int total = warmup + config.sampleEvents();
            deadlines = new long[total];
            sent = new long[total];
            published = new long[total];
        }

        int total() {
            return sent.length;
        }

        void published(long sequence) {
            long now = System.nanoTime();
            if (sequence != publishedCount + 1L || publishedCount >= total()) {
                throw new IllegalStateException("Publications must form one ordered prefix: " + sequence);
            }
            published[publishedCount++] = now;
        }
    }

    record Percentiles(long p50, long p99, long p999) {
        static Percentiles measure(long[] samples) {
            if (samples.length == 0) {
                return null;
            }
            Arrays.sort(samples);
            return new Percentiles(rank(samples, 0.50), rank(samples, 0.99), rank(samples, 0.999));
        }

        private static long rank(long[] sorted, double percentile) {
            return sorted[(int) Math.ceil(percentile * sorted.length) - 1];
        }
    }

    record Result(Samples samples, TopOfBookPublisher.Snapshot latest, long droppedA, long droppedB) {
        int offered() {
            return samples.total() - samples.warmup;
        }

        int applied() {
            return Math.max(0, samples.publishedCount - samples.warmup);
        }

        String outcome() {
            return latest.status() == STALE ? "STALE" : applied() == offered() ? "COMPLETE" : "INCOMPLETE";
        }

        int bookQueueFull() {
            return latest.failure().filter(failure -> failure.reason() == QUEUE_FULL).isPresent() ? 1 : 0;
        }

        double appliedRate() {
            if (applied() == 0) {
                return 0;
            }
            long end = Math.max(samples.measurementEnd, samples.published[samples.publishedCount - 1]);
            return (double) applied() * SECONDS.toNanos(1) / (end - samples.deadlines[samples.warmup]);
        }

        Percentiles sendToPublish() {
            return latenciesFrom(samples.sent);
        }

        Percentiles deadlineToPublish() {
            return latenciesFrom(samples.deadlines);
        }

        private Percentiles latenciesFrom(long[] start) {
            long[] latencies = new long[applied()];
            for (int i = 0; i < latencies.length; i++) {
                int index = samples.warmup + i;
                latencies[i] = samples.published[index] - start[index];
            }
            return Percentiles.measure(latencies);
        }

        void writeCsv(Path path) throws IOException {
            try (var writer = Files.newBufferedWriter(path)) {
                writer.write("sequence,send_lateness_ns,send_to_publication_ns,deadline_to_publication_ns\n");
                for (int i = samples.warmup; i < samples.total(); i++) {
                    writer.write((i + 1L) + "," + (samples.sent[i] - samples.deadlines[i]) + ",");
                    if (i < samples.publishedCount) {
                        writer.write((samples.published[i] - samples.sent[i]) + ","
                                + (samples.published[i] - samples.deadlines[i]));
                    } else {
                        writer.write(",");
                    }
                    writer.newLine();
                }
            }
        }
    }
}
