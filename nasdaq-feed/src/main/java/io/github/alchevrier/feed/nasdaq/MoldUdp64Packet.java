package io.github.alchevrier.feed.nasdaq;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Reusable framing descriptor, not an owner of datagram bytes. The parser
 * validates every block before the arbiter can emit any message, so a truncated
 * last block cannot cause partial delivery. A second pass walks message
 * offsets rather than allocating a table of views.
 *
 * <p>Absolute reads respect the caller's position and limit without changing
 * them. The descriptor drops its buffer reference when the lease ends.
 */
public final class MoldUdp64Packet {
    static final int SESSION_LENGTH = 10;
    private static final int HEADER_LENGTH = SESSION_LENGTH + Long.BYTES + Short.BYTES;
    private static final int END_OF_SESSION = 0xffff;
    private ByteBuffer datagram;
    private long firstSequence;
    private int messageCount;

    enum Framing { VALID, MALFORMED, WRONG_SESSION }

    Framing decode(ByteBuffer input, byte[] session) {
        if (input.remaining() < HEADER_LENGTH) {
            return Framing.MALFORMED;
        }
        int start = input.position();
        int limit = input.limit();
        long sequence = input.getLong(start + SESSION_LENGTH);
        if (input.order() != ByteOrder.BIG_ENDIAN) {
            sequence = Long.reverseBytes(sequence);
        }
        int count = unsignedShort(input, start + SESSION_LENGTH + Long.BYTES);
        if (count == END_OF_SESSION) {
            count = 0;
        }
        if (Long.compareUnsigned(sequence + count, sequence) < 0) {
            return Framing.MALFORMED;
        }

        int cursor = start + HEADER_LENGTH;
        for (int i = 0; i < count; i++) {
            if (limit - cursor < Short.BYTES) {
                return Framing.MALFORMED;
            }
            int length = unsignedShort(input, cursor);
            cursor += Short.BYTES;
            if (length > limit - cursor) {
                return Framing.MALFORMED;
            }
            cursor += length;
        }
        if (cursor != limit) {
            return Framing.MALFORMED;
        }
        for (int i = 0; i < SESSION_LENGTH; i++) {
            if (input.get(start + i) != session[i]) {
                return Framing.WRONG_SESSION;
            }
        }
        datagram = input;
        firstSequence = sequence;
        messageCount = count;
        return Framing.VALID;
    }

    long firstSequence() {
        return firstSequence;
    }

    long nextSequence() {
        return firstSequence + messageCount;
    }

    int firstMessageOffset() {
        return datagram.position() + HEADER_LENGTH;
    }

    int messageLengthAt(int offset) {
        return unsignedShort(datagram, offset);
    }

    void clear() {
        datagram = null;
    }

    private static int unsignedShort(ByteBuffer input, int offset) {
        short value = input.getShort(offset);
        return Short.toUnsignedInt(input.order() == ByteOrder.BIG_ENDIAN ? value : Short.reverseBytes(value));
    }
}
