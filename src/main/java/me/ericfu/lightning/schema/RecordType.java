package me.ericfu.lightning.schema;

import com.google.common.collect.ImmutableList;

import java.util.List;

public class RecordType {

    private final List<Field> fields;

    RecordType(List<Field> fields) {
        this.fields = ImmutableList.copyOf(fields);
    }

    public int getFieldCount() {
        return fields.size();
    }

    public Field getField(int index) {
        return fields.get(index);
    }

    public List<Field> getFields() {
        return fields;
    }
}
