package me.ericfu.lynx.sink;

import me.ericfu.lynx.schema.Schema;

public interface SchemalessSink extends Sink {

    /**
     * Set a required schema. Should be invoked after initialized
     */
    void provideSchema(Schema schema) throws IncompatibleClassChangeError;

}
