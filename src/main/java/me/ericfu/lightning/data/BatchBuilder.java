package me.ericfu.lightning.data;

import java.util.ArrayList;
import java.util.List;

public class BatchBuilder {

    private final List<Row> rows;

    public BatchBuilder(int initialCapacity) {
        this.rows = new ArrayList<Row>(initialCapacity);
    }

    public void addRow(Row row) {
        rows.add(row);
    }

    public Batch buildAndReset() {
        Batch batch = new Batch(rows.toArray(new Row[0]));
        rows.clear();
        return batch;
    }

    public int size() {
        return rows.size();
    }
}
