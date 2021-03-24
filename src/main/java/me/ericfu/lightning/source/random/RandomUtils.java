package me.ericfu.lightning.source.random;

import me.ericfu.lightning.data.ByteString;

import java.nio.charset.StandardCharsets;
import java.util.Random;

abstract class RandomUtils {

    /**
     * Generate a random string consist of printable characters in ASCII, ranging from 0x20 (space) to 0x7E (~)
     * <p>
     * Ref. https://en.wikipedia.org/wiki/ASCII#Printable_characters
     *
     * @param r   random object
     * @param len required length of string
     * @return generated random string in ByteString
     */
    public static ByteString createRandomAscii(Random r, int len) {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = (byte) (0x20 + r.nextInt(0x7F - 0x20));
        }
        return new ByteString(buf, StandardCharsets.UTF_8);
    }

    /**
     * Generate random binary data consist of any bytes
     *
     * @param r   random object
     * @param len required length of binary data
     * @return generated random data in ByteString
     */
    public static ByteString createRandomBinary(Random r, int len) {
        byte[] buf = new byte[len];
        r.nextBytes(buf);
        return new ByteString(buf, ByteString.CHARSET_BINARY);
    }

}
