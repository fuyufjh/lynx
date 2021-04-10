package me.ericfu.lynx.source.jdbc;

import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.StructType;

class JdbcSourceTable extends Table {

    public JdbcSourceTable(String name, StructType type) {
        super(name, type);
    }

    @Override
    public StructType getType() {
        return (StructType) super.getType();
    }
}
