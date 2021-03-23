package me.ericfu.lightning.schema;

import me.ericfu.lightning.data.ByteString;

public enum BasicType {

    //BOOLEAN(Boolean.class),

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

    private final Class<? extends Comparable<?>> clazz;

    private BasicType(Class<? extends Comparable<?>> clazz) {
        this.clazz = clazz;
    }

    public Class<? extends Comparable<?>> getClazz() {
        return clazz;
    }
}
