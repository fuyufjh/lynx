package me.ericfu.lynx.schema;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Collectors;

/**
 * Schema describes the table schema of multiple tables
 */
public final class Schema {

    /**
     * name to schema
     */
    private final Map<String, Table> tables; // TODO: preserve order of tables

    Schema(Map<String, Table> ts) {
        this.tables = ImmutableMap.copyOf(ts);
    }

    public Table getTable(String name) {
        return tables.get(name);
    }

    public Collection<Table> getTables() {
        return tables.values();
    }

    @Override
    public String toString() {
        return getTables().stream()
            .map(t -> t.getName() + ": " + t.getType().toString())
            .collect(Collectors.joining(", ", "{ ", " }"));
    }

    public static class Builder {

        private final Map<String, Table> tables;

        public Builder() {
            this.tables = new TreeMap<>();
        }

        public void addTable(Table table) {
            tables.put(table.getName(), table);
        }

        public Schema build() {
            return new Schema(tables);
        }
    }
}
