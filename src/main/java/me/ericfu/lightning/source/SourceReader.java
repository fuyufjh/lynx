package me.ericfu.lightning.source;

import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.exception.DataSourceException;

public interface SourceReader {

    void open() throws DataSourceException;

    /**
     * Read next batch of rows from data source
     *
     * @return next batch or null for EOF
     */
    RecordBatch readBatch() throws DataSourceException;

    void close() throws DataSourceException;

}
