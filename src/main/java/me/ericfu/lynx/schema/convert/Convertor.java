package me.ericfu.lynx.schema.convert;

/**
 * Convertor converts data to some specified version
 */
public interface Convertor {

    Object convert(Object in);

}
