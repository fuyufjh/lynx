package me.ericfu.lynx.schema;

import me.ericfu.lynx.data.ByteArray;

public enum BasicType {

    /**
     * booleans
     */
    BOOLEAN(Boolean.class),

    /**
     * 64-bit signed ints
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
     * arbitrarily long strings
     */
    STRING(String.class),

    /**
     * arbitrarily long byte arrays
     */
    BINARY(ByteArray.class),

    ;

    private final Class<?> clazz;

    BasicType(Class<?> clazz) {
        this.clazz = clazz;
    }

    public boolean isInstance(Object value) {
        return clazz.isInstance(value);
    }
}
