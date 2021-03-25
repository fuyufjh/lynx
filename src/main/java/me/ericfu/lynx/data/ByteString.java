package me.ericfu.lynx.data;

import java.io.InputStream;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

/**
 * ByteString stores strings in byte array and charset
 */
public final class ByteString implements Comparable<ByteString> {

    public static final Charset CHARSET_BINARY = StandardCharsets.ISO_8859_1;

    private final byte[] buf;
    private final int offset;
    private final int length;
    private final Charset charset;

    public ByteString(byte[] buf, int offset, int length, Charset charset) {
        this.buf = buf;
        this.offset = offset;
        this.length = length;
        this.charset = charset;
    }

    public ByteString(byte[] data, Charset charset) {
        this(data, 0, data.length, charset);
    }

    public int compareTo(ByteString other) {
        int lim = Math.min(length, other.length);
        int k = 0;
        while (k < lim) {
            byte c1 = buf[k + offset];
            byte c2 = other.buf[k + other.offset];
            if (c1 != c2) {
                return c1 - c2;
            }
            k++;
        }
        return length - other.length;
    }

    @Override
    public String toString() {
        return new String(buf, offset, length, charset);
    }

    public InputStream toBinaryStream() {
        return new InputStream() {
            private int index;

            @Override
            public int read() {
                if (index >= length) {
                    return -1;
                } else {
                    return buf[offset + (index++)];
                }
            }
        };
    }
}
