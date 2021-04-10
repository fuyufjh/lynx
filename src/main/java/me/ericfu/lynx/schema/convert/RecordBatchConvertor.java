package me.ericfu.lynx.schema.convert;

import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.data.RecordBatchBuilder;

public class RecordBatchConvertor {

    private final RecordConvertor convertor;

    private RecordBatchBuilder batchBuilder;

    public RecordBatchConvertor(RecordConvertor c) {
        this.convertor = c;
    }

    public RecordBatch convert(RecordBatch in) {
        if (batchBuilder == null) {
            batchBuilder = new RecordBatchBuilder(in.size());
        }
        for (Record record : in) {
            batchBuilder.addRow(convertor.convert(record));
        }
        return batchBuilder.buildAndReset();
    }
}
