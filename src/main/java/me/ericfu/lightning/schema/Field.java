package me.ericfu.lightning.schema;

public class Field {

    private final int ordinal;
    private final String name;
    private final BasicType type;

    public Field(int ordinal, String name, BasicType type) {
        this.ordinal = ordinal;
        this.name = name;
        this.type = type;
    }

    public int getOrdinal() {
        return ordinal;
    }

    public String getName() {
        return name;
    }

    public BasicType getType() {
        return type;
    }
}
