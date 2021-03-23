package me.ericfu.lightning.sink;

import me.ericfu.lightning.schema.RecordType;

public interface SchemalessSink {

    /**
     * Set a required schema. Should be invoked after initialized
     */
    void setSchema(RecordType schema) throws IncompatibleClassChangeError;

}
