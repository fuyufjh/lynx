package me.ericfu.lynx.sink;

import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;

public interface Sink {

    void init() throws DataSinkException;

    /**
     * Get schema. Should be invoked after initialized
     */
    Schema getSchema(Schema sourceSchema) throws IncompatibleClassChangeError;

    SinkWriter createWriter(Table table);
}
