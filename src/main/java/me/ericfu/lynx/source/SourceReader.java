package me.ericfu.lynx.source;

import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.checkpoint.SourceCheckpoint;

import javax.annotation.Nullable;

public interface SourceReader {

    /**
     * Open. Optionally continue from checkpoint
     */
    void open(@Nullable SourceCheckpoint checkpoint) throws DataSourceException;

    /**
     * Read next batch of rows from data source
     *
     * @return next batch or null for EOF
     */
    RecordBatch readBatch() throws DataSourceException;

    void close() throws DataSourceException;

    /**
     * Create a latest checkpoint
     */
    SourceCheckpoint checkpoint();
}
