package me.ericfu.lynx.source.random;

import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.StructType;

class RandomSourceTable extends Table {

    public RandomSourceTable(String name, StructType type) {
        super(name, type);
    }

    @Override
    public StructType getType() {
        return (StructType) super.getType();
    }
}
