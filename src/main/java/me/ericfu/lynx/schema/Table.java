package me.ericfu.lynx.schema;

import me.ericfu.lynx.schema.type.RecordType;

public class Table {

    private final String name;
    private final RecordType type;

    public Table(String name, RecordType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public RecordType getType() {
        return type;
    }
}
