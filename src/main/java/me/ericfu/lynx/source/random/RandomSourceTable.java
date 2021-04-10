package me.ericfu.lynx.source.random;

import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.StructType;

class RandomSourceTable extends Table {

    /**
     * Random generator for each column
     */
    private final RandomGenerator[] generators;

    public RandomSourceTable(String name, StructType type, RandomGenerator[] generators) {
        super(name, type);
        assert generators.length == type.getFieldCount();
        this.generators = generators;
    }

    @Override
    public StructType getType() {
        return (StructType) super.getType();
    }

    public RandomGenerator getGenerator(int i) {
        return generators[i];
    }
}
