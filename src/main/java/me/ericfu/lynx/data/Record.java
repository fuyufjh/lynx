package me.ericfu.lynx.data;

import lombok.NonNull;
import me.ericfu.lynx.schema.RecordType;

public class Record {

    private final RecordType type;
    private final Object[] values;

    public Record(@NonNull RecordType type, @NonNull Object[] values) {
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
}
