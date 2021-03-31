package me.ericfu.lynx.data;

import com.google.common.io.BaseEncoding;
import lombok.NonNull;

import java.io.InputStream;
import java.util.Arrays;

/**
 * ByteArray is a simple wrapper for <code>byte[]</code>
 */
public final class ByteArray {

    @NonNull
    private final byte[] bytes;

    public ByteArray(byte[] bytes) {
        this.bytes = bytes;
    }

    public byte[] getBytes() {
        return bytes;
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof ByteArray)) return false;
        return Arrays.equals(bytes, ((ByteArray) o).bytes);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(bytes);
    }

    @Override
    public String toString() {
        return "0x" + BaseEncoding.base16().lowerCase().encode(bytes);
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
