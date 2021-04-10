package me.ericfu.lynx.data;

import lombok.NonNull;

import java.util.Arrays;

public class Record {

    private final Object[] values;

    public Record(@NonNull Object... values) {
        this.values = values;
    }

    public Object getValue(int i) {
        return values[i];
    }

    public int size() {
        return values.length;
    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof Record)) return false;
        return Arrays.equals(values, ((Record) other).values);
    }

    @Override
    public int hashCode() {
        return Arrays.hashCode(values);
    }

    @Override
    public String toString() {
        return Arrays.toString(values);
    }
}
