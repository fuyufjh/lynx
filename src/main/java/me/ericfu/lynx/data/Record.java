package me.ericfu.lynx.data;

import lombok.NonNull;
import me.ericfu.lynx.schema.RecordType;

import java.util.Arrays;

public class Record {

    private final RecordType type; // TODO: consider remove this
    private final Object[] values;

    public Record(RecordType type, @NonNull Object[] values) {
        this.type = type;
        this.values = values;
    }

    public RecordType getType() {
        return type;
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
