package me.ericfu.lightning.source;

import me.ericfu.lightning.schema.Schema;

/**
 * Schemaless data source
 */
public interface SchemalessSource extends Source {

    /**
     * Set a required schema. Should be invoked after initialized
     */
    void provideSchema(Schema schema);

}
