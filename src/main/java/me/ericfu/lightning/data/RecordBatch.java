package me.ericfu.lightning.data;

import java.util.Iterator;

public class RecordBatch implements Iterable<Record> {

    private final Record[] records;

    RecordBatch(Record[] records) {
        this.records = records;
    }

    public int size() {
        return records.length;
    }

    @Override
    public Iterator<Record> iterator() {
        return new Iterator<Record>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < records.length;
            }

            @Override
            public Record next() {
                return records[index++];
            }
        };
    }
}
