package me.ericfu.lynx.source;

import me.ericfu.lynx.schema.Schema;

/**
 * Schemaless data source
 */
public interface SchemalessSource extends Source {

    /**
     * Set a required schema. Should be invoked after initialized
     */
    void provideSchema(Schema schema);

}
