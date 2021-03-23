package me.ericfu.lightning.schema;

/**
 * Convertor converts data to some specified version
 */
public interface Convertor {

    Object convert(Object in);

}
