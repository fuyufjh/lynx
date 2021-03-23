package me.ericfu.lightning.schema;

import java.util.ArrayList;
import java.util.List;

public class RecordTypeBuilder {

    private final List<Field> fields;

    public RecordTypeBuilder() {
        this.fields = new ArrayList<>();
    }

    public void addField(String name, BasicType type) {
        fields.add(new Field(name, type));
    }

    public RecordType build() {
        return new RecordType(fields);
    }
}
