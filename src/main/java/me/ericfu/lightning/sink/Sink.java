package me.ericfu.lightning.sink;

import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.Schema;
import me.ericfu.lightning.schema.Table;

public interface Sink {

    void init() throws DataSinkException;

    /**
     * Get schema. Should be invoked after initialized
     */
    Schema getSchema();

    SinkWriter createWriter(Table table);
}
