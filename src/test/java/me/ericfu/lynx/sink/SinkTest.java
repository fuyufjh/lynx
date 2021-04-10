package me.ericfu.lynx.sink;

import me.ericfu.lynx.PluginTest;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.model.checkpoint.SinkCheckpoint;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.schema.type.TupleType;

import java.util.List;

public abstract class SinkTest extends PluginTest {

    protected final Schema sourceSchema;

    {
        // Build a source schema with table 't1' containing all basic types
        StructType.Builder typeBuilder = new StructType.Builder();
        for (BasicType t : BasicType.values()) {
            // e.g. 'boolean_col'
            typeBuilder.addField(t.name().toLowerCase() + "_col", t);
        }
        StructType type = typeBuilder.build();

        Schema.Builder schemaBuilder = new Schema.Builder();
        schemaBuilder.addTable(new Table("t1", type));
        sourceSchema = schemaBuilder.build();
    }

    /**
     * Write all records to sink
     */
    protected void writeAllRecords(Sink sink, Table table) throws Exception {
        writeAllRecords(sink, table, null);
    }

    /**
     * Write all records to sink continued with a checkpoint
     */
    protected void writeAllRecords(Sink sink, Table table,
                                   List<SinkCheckpoint> checkpoints) throws Exception {
        SinkWriter writer0 = sink.createWriter(table, 0);
        writer0.open(checkpoints != null ? checkpoints.get(0) : null);

        writer0.writeBatch(new RecordBatch(recordAt(table, 0), recordAt(table, 1), recordAt(table, 2)));
        writer0.writeBatch(new RecordBatch(recordAt(table, 3), recordAt(table, 4)));

        SinkWriter writer1 = sink.createWriter(table, 1);
        writer1.open(checkpoints != null ? checkpoints.get(1) : null);

        writer1.writeBatch(new RecordBatch(recordAt(table, 5), recordAt(table, 6), recordAt(table, 7)));
        writer1.writeBatch(new RecordBatch(recordAt(table, 8), recordAt(table, 9)));
    }

    private Record recordAt(Table table, int i) {
        if (table.getType() instanceof TupleType) {
            final List<Field> fields = ((TupleType) table.getType()).getFields();
            Object[] values = fields.stream().map(f -> {
                switch (f.getType()) {
                case BOOLEAN:
                    return RECORD[i].booleanVal;
                case INT:
                    return RECORD[i].intVal;
                case LONG:
                    return RECORD[i].longVal;
                case FLOAT:
                    return RECORD[i].floatVal;
                case DOUBLE:
                    return RECORD[i].doubleVal;
                case STRING:
                    return RECORD[i].stringVal;
                case BINARY:
                    return RECORD[i].binaryVal;
                default:
                    throw new AssertionError();
                }
            }).toArray();
            return new Record(values);
        } else {
            throw new AssertionError("not implemented");
        }
    }
}
