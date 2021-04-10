package me.ericfu.lynx.schema;

import me.ericfu.lynx.exception.IncompatibleSchemaException;
import me.ericfu.lynx.schema.convert.Convertors;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;

public abstract class SchemaUtils {

    /**
     * Check source schema is compatible with sink schema
     *
     * @param source schema of data source
     * @param sink   schema of data sink
     * @throws IncompatibleSchemaException if not compatible
     */
    public static void checkCompatible(Schema source, Schema sink) throws IncompatibleSchemaException {
        for (Table sourceTable : source.getTables()) {
            Table sinkTable = sink.getTable(sourceTable.getName());
            if (sinkTable == null) {
                throw new IncompatibleSchemaException("source table '" + sourceTable.getName() + "' not exist on sink");
            }
            checkCompatible(sourceTable.getType(), sinkTable.getType());
        }
    }

    /**
     * Check record type of source is compatible with sink.
     * <p>
     * The record types is compatible if each column in source exists in sink side
     * and the column type is compatible.
     * <p>
     * By default the missing field in sink but not in source will be filled with NULL.
     *
     * @param source record type of data source
     * @param sink   record type of data sink
     * @throws IncompatibleSchemaException if not compatible
     */
    public static void checkCompatible(StructType source, StructType sink)
        throws IncompatibleSchemaException {
        // Check type of each field
        for (int i = 0; i < source.getFieldCount(); i++) {
            final Field sourceField = source.getField(i);
            final Field sinkField = sink.getField(sourceField.getName());
            if (sinkField == null) {
                throw new IncompatibleSchemaException("source field '" + sourceField.getName() + "' not found in sink");
            }
            checkCompatible(sourceField.getType(), sinkField.getType());
        }
    }

    /**
     * Check a field type of source is compatible with sink.
     * <p>
     * Currently the rule is quite simple: if there exists a convertor could convert type from source to sink,
     * these types are compatible, otherwise not.
     *
     * @param source field type of data source
     * @param sink   field type of data sink
     * @throws IncompatibleSchemaException if not compatible
     */
    private static void checkCompatible(BasicType source, BasicType sink)
        throws IncompatibleSchemaException {
        if (source != sink && Convertors.getConvertor(source, sink) == null) {
            throw new IncompatibleSchemaException(source + " cannot be converted to " + sink);
        }
    }
}
