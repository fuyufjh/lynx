package me.ericfu.lynx.data;

import lombok.NonNull;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.RecordType;

public class RecordBuilder {

    private final RecordType type;
    private final Object[] values;

    public RecordBuilder(@NonNull RecordType type) {
        this.type = type;
        this.values = new Object[type.getFieldCount()];
    }

    public void set(String field, Object value) {
        final Field f = type.getField(field);
        assert f.getType().isInstance(value);
        values[f.getOrdinal()] = value;
    }

    public void set(int ordinal, Object value) {
        assert type.getField(ordinal).getType().isInstance(value);
        values[ordinal] = value;
    }

    /**
     * Build a Record. The fields not set will be treat as NULL
     */
    public Record build() {
        return new Record(type, values);
    }
}