package me.ericfu.lightning.schema;

import me.ericfu.lightning.exception.IncompatibleSchemaException;

public abstract class SchemaUtils {

    /**
     * Combine record type of source and sink for transferring data
     * <p>
     * Currently the rule is quite simple: if the fields from source and sink share the same data type, that data type
     * will be chosen as combined type. Otherwise STRING type will be chosen.
     *
     * @param source record type of data source
     * @param sink record type of data sink
     * @return the combined data type
     * @throws IncompatibleSchemaException if the required conversion is not allowed
     */
    public static RecordType combine(RecordType source, RecordType sink)
        throws IncompatibleSchemaException {
        if (source.getFieldCount() != sink.getFieldCount()) {
            throw new IncompatibleSchemaException("field counts not matches");
        }

        RecordTypeBuilder builder = new RecordTypeBuilder();
        for (int i = 0; i < source.getFieldCount(); i++) {
            // use sink field name
            if (source.getField(i).getType() == sink.getField(i).getType()) {
                final Field f = sink.getField(i);
                builder.addField(f.getName(), f.getType());
            } else {
                builder.addField(sink.getField(i).getName(), BasicType.STRING);
            }
        }
        return builder.build();
    }

}
