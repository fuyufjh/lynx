package me.ericfu.lightning.sink;

import me.ericfu.lightning.schema.RecordType;

public interface SchemalessSink extends Sink {

    /**
     * Set a required schema. Should be invoked after initialized
     */
    void provideSchema(RecordType schema) throws IncompatibleClassChangeError;

}
