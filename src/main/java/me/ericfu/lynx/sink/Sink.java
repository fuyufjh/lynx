package me.ericfu.lynx.sink;

import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.exception.IncompatibleSchemaException;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;

public interface Sink {

    /**
     * Provide source schema and do initialize
     */
    void init(Schema sourceSchema) throws IncompatibleSchemaException, DataSinkException;

    /**
     * Get schema. Should be invoked after initialized
     */
    Schema getSchema();

    /**
     * Create writer with a unique number
     *
     * @param table sink table
     * @param no    an incremental and unique integer representing the written partition
     */
    SinkWriter createWriter(Table table, int no);
}
