package me.ericfu.lightning.source;

import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.Schema;
import me.ericfu.lightning.schema.Table;

/**
 * Data source
 */
public interface Source {

    void init() throws DataSourceException;

    /**
     * Get the table schema. Should be invoked after initialized
     */
    Schema getSchema();

    /**
     * Create readers
     */
    Iterable<SourceReader> createReaders(Table table);

}
