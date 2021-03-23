package me.ericfu.lightning.data;

import java.util.Iterator;

public class Batch implements Iterable<Row> {

    private final Row[] rows;

    public Batch(Row[] rows) {
        this.rows = rows;
    }

    public int size() {
        return rows.length;
    }

    @Override
    public Iterator<Row> iterator() {
        return new Iterator<Row>() {
            private int index;

            @Override
            public boolean hasNext() {
                return index < rows.length;
            }

            @Override
            public Row next() {
                return rows[index++];
            }
        };
    }
}
