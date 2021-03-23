package me.ericfu.lightning.sink;

import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.exception.DataSinkException;

public interface SinkWriter {

    void open() throws DataSinkException;

    /**
     * Read next batch of rows from data source
     *
     * @return next batch or null for EOF
     */
    void writeBatch(RecordBatch batch) throws DataSinkException;

    void close() throws DataSinkException;

}