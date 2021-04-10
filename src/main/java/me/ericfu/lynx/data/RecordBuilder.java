package me.ericfu.lynx.data;

import lombok.NonNull;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.type.TupleType;

public class RecordBuilder {

    private final TupleType type;
    private final Object[] values;

    public RecordBuilder(@NonNull TupleType type) {
        this.type = type;
        this.values = new Object[type.getFieldCount()];
    }

    public void set(int ordinal, Object value) {
        final Field f = type.getField(ordinal);
        assert f.getType().isInstance(value) :
            "type mismatch: expect " + f.getType().getClazz().getName() + " but got " + value.getClass().getName();
        values[ordinal] = value;
    }

    /**
     * Build a Record. The fields not set will be treat as NULL
     */
    public Record build() {
        return new Record(values);
    }
}
