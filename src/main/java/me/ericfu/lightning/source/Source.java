package me.ericfu.lightning.source;

import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.RecordType;

import java.util.List;

/**
 * Data source
 */
public interface Source {

    void init() throws DataSourceException;

    /**
     * Get the table schema. Should be invoked after initialized
     */
    RecordType getSchema();

    /**
     * Create readers
     */
    List<SourceReader> createReaders();

}
