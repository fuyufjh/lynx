package me.ericfu.lightning.schema;

import me.ericfu.lightning.data.ByteString;

public enum BasicType {

    //TODO: BOOLEAN(Boolean.class),

    /**
     * 64 bit signed ints
     */
    INT64(Long.class),

    /**
     * IEEE 32-bit floating point values
     */
    FLOAT(Float.class),

    /**
     * IEEE 64-bit floating point values
     */
    DOUBLE(Double.class),

    /**
     * arbitrarily long byte arrays
     */
    STRING(ByteString.class), // TODO: split string and bytes

    ;

    private final Class<?> clazz;

    BasicType(Class<?> clazz) {
        this.clazz = clazz;
    }

    public boolean isInstance(Object value) {
        return clazz.isInstance(value);
    }
}
