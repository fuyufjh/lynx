package me.ericfu.lynx.schema;

import java.util.Map;
import java.util.TreeMap;

public class SchemaBuilder {

    private final Map<String, Table> tables;

    public SchemaBuilder() {
        this.tables = new TreeMap<>();
    }

    public void addTable(Table table) {
        tables.put(table.getName(), table);
    }

    public Schema build() {
        return new Schema(tables);
    }
}
