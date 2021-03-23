package me.ericfu.lightning.source;

import me.ericfu.lightning.schema.RecordType;

/**
 * Data source with fixed schema
 */
public interface SchemaSource extends Source {

    /**
     * Get the table schema. Should be invoked after initialized
     */
    RecordType getSchema();

}
