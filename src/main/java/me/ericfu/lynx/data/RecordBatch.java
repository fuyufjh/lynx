package me.ericfu.lynx.data;

import javax.validation.constraints.NotNull;
import java.util.Iterator;

public class RecordBatch implements Iterable<Record> {

    private final Record[] records;

    public RecordBatch(@NotNull Record... records) {
        this.records = records;
    }

    public int size() {
        return records.length;
    }

    public Record getRecord(int i) {
        return records[i];
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
