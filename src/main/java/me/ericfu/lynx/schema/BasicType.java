package me.ericfu.lynx.schema;

import me.ericfu.lynx.data.ByteArray;

public enum BasicType {

    /**
     * boolean
     */
    BOOLEAN(Boolean.class),

    /**
     * 32-bit signed integer
     */
    INT(Integer.class),

    /**
     * 64-bit signed integer
     */
    LONG(Long.class),

    /**
     * IEEE 32-bit floating-point number
     */
    FLOAT(Float.class),

    /**
     * IEEE 64-bit floating-point number
     */
    DOUBLE(Double.class),

    /**
     * unicode character sequence
     */
    STRING(String.class),

    /**
     * sequence of bytes
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
