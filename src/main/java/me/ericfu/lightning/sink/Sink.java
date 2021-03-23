package me.ericfu.lightning.sink;

import me.ericfu.lightning.data.Batch;
import me.ericfu.lightning.exception.DataSinkException;

public interface Sink {

    void open() throws DataSinkException;

    /**
     * Read next batch of rows from data source
     *
     * @return next batch or null for EOF
     */
    void writeBatch(Batch batch) throws DataSinkException;

    void close() throws DataSinkException;

}
