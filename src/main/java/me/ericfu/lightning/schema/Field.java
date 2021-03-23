package me.ericfu.lightning.schema;

public class Field {

    private final String name;

    private final BasicType type;

    public Field(String name, BasicType type) {
        this.name = name;
        this.type = type;
    }

    public String getName() {
        return name;
    }

    public BasicType getType() {
        return type;
    }
}
