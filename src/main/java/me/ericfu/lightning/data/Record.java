package me.ericfu.lightning.data;

import lombok.NonNull;
import me.ericfu.lightning.schema.RecordType;

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
}
