package io.github.alchevrier.feed.app

import io.github.alchevrier.feed.book.OrderBook.PriceLevel
import io.github.alchevrier.feed.book.OrderBook.TopOfBook
import spock.lang.Specification
import spock.lang.TempDir

import java.nio.file.Files
import java.nio.file.Path

import static io.github.alchevrier.feed.app.LoopbackBaseline.BookLayout.PACKED
import static io.github.alchevrier.feed.app.LoopbackBaseline.BookLayout.REFERENCE
import static io.github.alchevrier.feed.app.LoopbackBaseline.Load.BURSTY
import static io.github.alchevrier.feed.app.LoopbackBaseline.Load.STEADY

class LoopbackBaselineSpec extends Specification {
    @TempDir
    Path output

    def "paces #load independently of publication"() {
        expect:
        (0..<indexes.size()).collect { load.offsetNanos(it, 1000) } == indexes.collect { it * 1_000_000L }

        where:
        load   | indexes
        STEADY | [0, 1, 2]
        BURSTY | [0] * 16 + [16, 16]
    }

    def "nearest-rank percentiles leave an empty sample distinguishable from zero latency"() {
        expect:
        LoopbackBaseline.Percentiles.measure(samples as long[]) == expected

        where:
        samples        | expected
        []             | null
        [0]            | new LoopbackBaseline.Percentiles(0, 0, 0)
        [40, 10, 30, 20] | new LoopbackBaseline.Percentiles(20, 40, 40)
    }

    def "the #layout book records each applied snapshot once despite duplicated #load datagrams"() {
        when:
        def result = LoopbackBaseline.run(load, new LoopbackBaseline.Config(32, warmup, 1), layout)
        def csv = output.resolve('samples.csv')
        result.writeCsv(csv)
        def lines = Files.readAllLines(csv)
        def rows = lines.drop(1)*.split(',')

        then:
        result.offered() == 32
        result.applied() == 32
        result.outcome() == 'COMPLETE'
        result.latest().quote() == quote
        result.bookQueueFull() == 0
        result.sendToPublish() != null
        result.deadlineToPublish().p99() >= result.sendToPublish().p99()
        lines.first() == 'sequence,send_lateness_ns,send_to_publication_ns,deadline_to_publication_ns'
        rows.collect { it[0] as int } == ((32 * warmup + 1)..(32 * (warmup + 1))).toList()
        rows.every {
            def (_, lateness, sendToPublish, deadlineToPublish) = it.collect { it as long }
            lateness >= 0 && sendToPublish >= 0 && deadlineToPublish == lateness + sendToPublish
        }

        where:
        layout << [REFERENCE, PACKED]
        combined:
        load   | warmup | quote
        STEADY | 1      | new TopOfBook(null, new PriceLevel(100_021, 100))
        BURSTY | 0      | new TopOfBook(new PriceLevel(100_010, 60), null)
    }
}
