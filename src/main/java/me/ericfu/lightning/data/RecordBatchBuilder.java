package me.ericfu.lightning.data;

import java.util.ArrayList;
import java.util.List;

public class RecordBatchBuilder {

    private final List<Record> records;

    public RecordBatchBuilder(int initialCapacity) {
        this.records = new ArrayList<Record>(initialCapacity);
    }

    public void addRow(Record record) {
        records.add(record);
    }

    public RecordBatch buildAndReset() {
        RecordBatch batch = new RecordBatch(records.toArray(new Record[0]));
        records.clear();
        return batch;
    }

    public int size() {
        return records.size();
    }
}
