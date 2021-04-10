package me.ericfu.lynx.schema;

import me.ericfu.lynx.schema.type.StructType;

public final class Table {

    private final String name;
    private final StructType type;

    public Table(String name, StructType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public StructType getType() {
        return type;
    }
}
