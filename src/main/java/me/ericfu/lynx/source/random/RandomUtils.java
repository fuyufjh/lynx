package me.ericfu.lynx.source.random;

import me.ericfu.lynx.data.ByteArray;

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
    public static String createRandomAscii(Random r, int len) {
        byte[] buf = new byte[len];
        for (int i = 0; i < len; i++) {
            buf[i] = (byte) (0x20 + r.nextInt(0x7F - 0x20));
        }
        return new String(buf, StandardCharsets.ISO_8859_1);
    }

    /**
     * Generate random binary data consist of any bytes
     *
     * @param r   random object
     * @param len required length of binary data
     * @return generated random data in ByteString
     */
    public static ByteArray createRandomBinary(Random r, int len) {
        byte[] buf = new byte[len];
        r.nextBytes(buf);
        return new ByteArray(buf);
    }

}
