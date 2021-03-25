package me.ericfu.lynx.schema;

public final class Table {

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
