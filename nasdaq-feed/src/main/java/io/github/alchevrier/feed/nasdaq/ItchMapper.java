package io.github.alchevrier.feed.nasdaq;

import io.github.alchevrier.feed.core.MarketMessage.Side;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Decodes TotalView-ITCH 5.0 order updates for one stock locate. The mapper
 * borrows one payload range for the call and forwards values, not buffer views.
 * The target owns event storage; the caller owns sequencing.
 */
public final class ItchMapper {
    private static final int STOCK_LOCATE = 1;
    private static final int ORDER_REFERENCE = 11;
    private static final int ADD_SIDE = 19;
    private static final int ADD_SHARES = 20;
    private static final int ADD_PRICE = 32;
    private static final int EXECUTED_SHARES = 19;

    private final int stockLocate;

    public ItchMapper(int stockLocate) {
        if (stockLocate < 0 || stockLocate > 0xffff) {
            throw new IllegalArgumentException("Stock locate must fit in an unsigned 16-bit field");
        }
        this.stockLocate = stockLocate;
    }

    /**
     * Reads one complete ITCH payload without changing the source buffer.
     * Known messages that do not affect the tracked book produce no event.
     *
     * @throws IllegalArgumentException for an invalid range, unknown type,
     *         incorrect wire length, or invalid side or shares in a mapped event
     * @throws UnsupportedOperationException for an unsupported mutation of the tracked book
     */
    public void decode(ByteBuffer source, int offset, int length, ItchEventTarget target) {
        if (length <= 0 || offset < source.position() || offset > source.limit() - length) {
            throw new IllegalArgumentException("ITCH payload is outside the readable buffer range");
        }

        byte type = source.get(offset);
        if (length != wireLength(type)) {
            throw new IllegalArgumentException("Incorrect ITCH wire length for type " + (char) type + ": " + length);
        }

        switch (type) {
            case 'A', 'E', 'D', 'F', 'C', 'X', 'U' -> {
                boolean reverse = source.order() != ByteOrder.BIG_ENDIAN;
                short locate = source.getShort(offset + STOCK_LOCATE);
                if (Short.toUnsignedInt(reverse ? Short.reverseBytes(locate) : locate) == stockLocate) {
                    decodeOrder(source, offset, type, reverse, target);
                }
            }
            default -> { } // Recognized non-book type. Its sequence still belongs to the stream.
        }
    }

    private static void decodeOrder(ByteBuffer source, int offset, byte type, boolean reverse, ItchEventTarget target) {
        long reference = source.getLong(offset + ORDER_REFERENCE);
        if (reverse) {
            reference = Long.reverseBytes(reference);
        }
        switch (type) {
            case 'A' -> {
                Side side = switch (source.get(offset + ADD_SIDE)) {
                    case 'B' -> Side.BUY;
                    case 'S' -> Side.SELL;
                    default -> throw new IllegalArgumentException("Invalid ITCH add side");
                };
                long shares = positiveShares(source, offset + ADD_SHARES, reverse);
                long price = unsignedInt(source, offset + ADD_PRICE, reverse);
                target.add(reference, side, shares, price);
            }
            case 'E' -> target.execute(reference, positiveShares(source, offset + EXECUTED_SHARES, reverse));
            case 'D' -> target.delete(reference);
            default -> throw new UnsupportedOperationException("Unsupported ITCH order mutation: " + (char) type);
        }
    }

    private static long positiveShares(ByteBuffer source, int offset, boolean reverse) {
        long shares = unsignedInt(source, offset, reverse);
        if (shares == 0) {
            throw new IllegalArgumentException("ITCH share count must be positive");
        }
        return shares;
    }

    private static long unsignedInt(ByteBuffer source, int offset, boolean reverse) {
        int value = source.getInt(offset);
        return Integer.toUnsignedLong(reverse ? Integer.reverseBytes(value) : value);
    }

    private static int wireLength(byte type) {
        return switch (type) {
            case 'A', 'C' -> 36;
            case 'E' -> 31;
            case 'D', 'B' -> 19;
            case 'F', 'Q' -> 40;
            case 'X' -> 23;
            case 'U', 'V', 'J' -> 35;
            case 'S', 'W' -> 12;
            case 'R' -> 39;
            case 'H' -> 25;
            case 'Y', 'N' -> 20;
            case 'L' -> 26;
            case 'K' -> 28;
            case 'h' -> 21;
            case 'P' -> 44;
            case 'I' -> 50;
            case 'O' -> 48;
            default -> throw new IllegalArgumentException("Unknown ITCH message type: " + Byte.toUnsignedInt(type));
        };
    }
}
