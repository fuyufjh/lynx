package me.ericfu.lightning.source;

import me.ericfu.lightning.data.Batch;
import me.ericfu.lightning.exception.DataSourceException;

/**
 * Data source
 */
public interface Source {

    void open() throws DataSourceException;

    /**
     * Read next batch of rows from data source
     *
     * @return next batch or null for EOF
     */
    Batch readBatch() throws DataSourceException;

    void close() throws DataSourceException;

}
