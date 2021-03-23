package me.ericfu.lightning.source;

import me.ericfu.lightning.schema.RecordType;

/**
 * Schemaless data source
 */
public interface SchemalessSource extends Source {

    /**
     * Set a required schema. Should be invoked after initialized
     */
    void setSchema(RecordType schema);

}
