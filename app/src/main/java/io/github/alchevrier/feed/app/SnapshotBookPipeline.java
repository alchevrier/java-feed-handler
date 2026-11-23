package io.github.alchevrier.feed.app;

import io.github.alchevrier.feed.book.OrderBook;
import io.github.alchevrier.feed.book.OrderBook.TopOfBookBuffer;
import io.github.alchevrier.feed.nasdaq.ItchBookHandoff;
import io.github.alchevrier.feed.nasdaq.MoldUdp64Arbiter.Leg;

import java.nio.ByteBuffer;
import java.util.Objects;

/**
 * Wires a single-owner book and the SPSC handoff to the strategy view.
 * Publish terminal status from either pipeline thread, including when a
 * handoff operation throws, without waiting for the book ring to drain.
 */
final class SnapshotBookPipeline {
    private final ItchBookHandoff handoff;
    private final OrderBook book;
    private final TopOfBookPublisher publisher;
    private final TopOfBookBuffer quote = new TopOfBookBuffer();
    private final ItchBookHandoff.BookEventConsumer apply;

    SnapshotBookPipeline(ItchBookHandoff handoff, OrderBook book, TopOfBookPublisher publisher) {
        this.handoff = Objects.requireNonNull(handoff, "handoff");
        this.book = Objects.requireNonNull(book, "book");
        this.publisher = Objects.requireNonNull(publisher, "publisher");
        apply = (sequence, event) -> {
            this.book.apply(event);
            this.book.readTopOfBook(quote);
            this.publisher.publishApplied(sequence, quote);
        };
    }

    void accept(Leg leg, ByteBuffer datagram, long nowNanos) {
        try {
            handoff.accept(leg, datagram, nowNanos);
        } finally {
            publishFailure();
        }
    }

    void checkTimeout(long nowNanos) {
        handoff.checkTimeout(nowNanos);
        publishFailure();
    }

    boolean tryApplyNext() {
        try {
            return handoff.tryConsume(apply);
        } finally {
            publishFailure();
        }
    }

    private void publishFailure() {
        ItchBookHandoff.Stale failure = handoff.stale().orElse(null);
        if (failure != null) {
            publisher.publishStale(failure);
        }
    }
}
