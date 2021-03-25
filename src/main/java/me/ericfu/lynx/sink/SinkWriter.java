package me.ericfu.lynx.sink;

import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.exception.DataSinkException;

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
