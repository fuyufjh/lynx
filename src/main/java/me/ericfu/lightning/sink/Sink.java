package me.ericfu.lightning.sink;

import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.RecordType;

public interface Sink {

    void init() throws DataSinkException;

    /**
     * Get schema. Should be invoked after initialized
     */
    RecordType getSchema();

    SinkWriter createWriter(int partNo);
}
