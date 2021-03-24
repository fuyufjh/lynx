package me.ericfu.lightning.sink;

import me.ericfu.lightning.schema.Schema;

public interface SchemalessSink extends Sink {

    /**
     * Set a required schema. Should be invoked after initialized
     */
    void provideSchema(Schema schema) throws IncompatibleClassChangeError;

}
