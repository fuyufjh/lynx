package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.StructType;

class JdbcSinkTable extends Table {

    /**
     * Insert statement templates for each table
     */
    private final String insertTemplate;

    public JdbcSinkTable(String name, StructType type, String insertTemplate) {
        super(name, type);
        this.insertTemplate = insertTemplate;
    }

    @Override
    public StructType getType() {
        return (StructType) super.getType();
    }

    public String getInsertTemplate() {
        return insertTemplate;
    }
}
