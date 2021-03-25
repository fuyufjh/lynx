package me.ericfu.lynx.schema;

import me.ericfu.lynx.exception.IncompatibleSchemaException;

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
     * Check record type of source is compatible with sink
     * <p>
     * Currently the rule is quite simple: if there is a convertor able to convert type from source to sink,
     * these types are compatible, otherwise not.
     *
     * @param source record type of data source
     * @param sink   record type of data sink
     * @throws IncompatibleSchemaException if not compatible
     */
    public static void checkCompatible(RecordType source, RecordType sink)
        throws IncompatibleSchemaException {
        if (source.getFieldCount() != sink.getFieldCount()) {
            throw new IncompatibleSchemaException("field counts not matches");
        }

        // Check type of each field
        for (int i = 0; i < source.getFieldCount(); i++) {
            final BasicType sourceFieldType = source.getField(i).getType();
            final BasicType sinkFieldType = sink.getField(i).getType();
            if (sourceFieldType != sinkFieldType
                && Convertors.getConvertor(sourceFieldType, sinkFieldType) == null) {
                throw new IncompatibleSchemaException(sourceFieldType + " cannot be converted to " + sinkFieldType);
            }
        }
    }

}
