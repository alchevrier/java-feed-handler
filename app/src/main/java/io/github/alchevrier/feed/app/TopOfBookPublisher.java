package io.github.alchevrier.feed.app;

import io.github.alchevrier.feed.book.OrderBook.TopOfBook;
import io.github.alchevrier.feed.book.OrderBook.TopOfBookBuffer;
import io.github.alchevrier.feed.nasdaq.ItchBookHandoff.Stale;

import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Publishes the latest immutable view for one instrument and one run.
 * Strategy threads may retain a captured view without holding up the book
 * writer. They must read again to observe health or quote changes.
 */
public final class TopOfBookPublisher {
    private final AtomicReference<Snapshot> latest = new AtomicReference<>(
            new Snapshot(new TopOfBook(null, null), OptionalLong.empty(), Status.NOT_READY, Optional.empty()));

    public Snapshot latest() {
        return latest.get();
    }

    /** Single book writer only. A competing write can only make the run stale. */
    void publishApplied(long sequence, TopOfBookBuffer quote) {
        Snapshot previous = latest.get();
        if (previous.status() != Status.STALE) {
            latest.compareAndSet(previous, new Snapshot(quote.snapshot(previous.quote()),
                    OptionalLong.of(sequence), Status.LIVE, Optional.empty()));
        }
    }

    /** Either pipeline thread can publish a terminal failure independently of the book ring. */
    void publishStale(Stale failure) {
        Optional<Stale> terminal = Optional.of(failure);
        Snapshot previous;
        do {
            previous = latest.get();
            if (previous.status() == Status.STALE) {
                return;
            }
        } while (!latest.compareAndSet(previous, new Snapshot(previous.quote(), previous.lastAppliedSequence(),
                Status.STALE, terminal)));
    }

    public enum Status { NOT_READY, LIVE, STALE }

    /** Quote and applied sequence describe the same book state; failure has its own sequence. */
    public record Snapshot(TopOfBook quote, OptionalLong lastAppliedSequence, Status status,
                           Optional<Stale> failure) {}
}
