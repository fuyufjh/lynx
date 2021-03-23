package me.ericfu.lightning.data;

public class Row {

    private final Object[] values;

    public Row(Object... values) {
        this.values = values;
    }

    public Object getValue(int i) {
        return values[i];
    }
}
