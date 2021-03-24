package me.ericfu.lightning.sink;

import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.RecordType;

import java.util.List;

public interface Sink {

    void init() throws DataSinkException;

    /**
     * Get schema. Should be invoked after initialized
     */
    RecordType getSchema();

    List<SinkWriter> createWriters(int n);
}
