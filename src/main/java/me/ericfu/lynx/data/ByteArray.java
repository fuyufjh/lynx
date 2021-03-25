package me.ericfu.lynx.data;

import java.io.InputStream;

/**
 * ByteArray is a simple wrapper for <code>byte[]</code>
 */
public final class ByteArray {

    private final byte[] bytes;

    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    public InputStream toBinaryStream() {
        return new InputStream() {
            private int index;

            @Override
            public int read() {
                if (index >= bytes.length) {
                    return -1;
                } else {
                    return bytes[index++];
                }
            }
        };
    }
}
