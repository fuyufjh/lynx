package me.ericfu.lightning.schema;

import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.data.RecordBatchBuilder;

public class RecordBatchConvertor {

    private final RecordConvertor convertor;

    private RecordBatchBuilder batchBuilder;

    public RecordBatchConvertor(RecordConvertor c) {
        this.convertor = c;
    }

    public RecordBatch convert(RecordBatch in) {
        if (convertor.isIdentical()) {
            return in;
        }
        if (batchBuilder == null) {
            batchBuilder = new RecordBatchBuilder(in.size());
        }
        for (Record record : in) {
            batchBuilder.addRow(convertor.convert(record));
        }
        return batchBuilder.buildAndReset();
    }
}
