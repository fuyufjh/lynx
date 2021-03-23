package me.ericfu.lightning.sink;

import me.ericfu.lightning.schema.RecordType;

public interface SchemaSink extends Sink {

    /**
     * Get schema. Should be invoked after initialized
     */
    RecordType getSchema();

}
