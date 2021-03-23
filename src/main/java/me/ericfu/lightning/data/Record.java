package me.ericfu.lightning.data;

import me.ericfu.lightning.schema.RecordType;

public class Record {

    private final RecordType type;
    private final Object[] values;

    public Record(RecordType type, Object[] values) {
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
