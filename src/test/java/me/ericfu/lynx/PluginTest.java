package me.ericfu.lynx;

import me.ericfu.lynx.data.ByteArray;
import me.ericfu.lynx.source.random.RandomUtils;

import java.util.Random;

public abstract class PluginTest {

    protected static final int NUM_RECORDS = 10;

    protected static final RandomRecord[] RECORD;

    /**
     * Fix a random seed so the generated data is always same
     */
    private static final Random r = new Random(0);

    static {
        RECORD = new RandomRecord[NUM_RECORDS];

        for (int i = 0; i < NUM_RECORDS; i++) {
            RECORD[i] = new RandomRecord(
                nullOr(r.nextBoolean()),
                nullOr(r.nextInt()),
                nullOr(r.nextLong()),
                nullOr(r.nextFloat()),
                nullOr(r.nextDouble()),
                nullOr(RandomUtils.randomAsciiString(r, 20)),
                nullOr(RandomUtils.randomBinary(r, 20))
            );
        }
    }

    protected static class RandomRecord {
        public final Boolean booleanVal;
        public final Integer intVal;
        public final Long longVal;
        public final Float floatVal;
        public final Double doubleVal;
        public final String stringVal;
        public final ByteArray binaryVal;

        private RandomRecord(Boolean booleanVal,
                             Integer intVal,
                             Long longVal,
                             Float floatVal,
                             Double doubleVal,
                             String stringVal,
                             ByteArray binaryVal) {
            this.booleanVal = booleanVal;
            this.intVal = intVal;
            this.longVal = longVal;
            this.floatVal = floatVal;
            this.doubleVal = doubleVal;
            this.stringVal = stringVal;
            this.binaryVal = binaryVal;
        }
    }


    private static <T> T nullOr(T value) {
        if (r.nextDouble() < 0.1) {
            return null;
        } else {
            return value;
        }
    }
}
