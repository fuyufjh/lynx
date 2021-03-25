package me.ericfu.lynx.source;

import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;

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
