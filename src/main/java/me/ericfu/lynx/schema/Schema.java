package me.ericfu.lynx.schema;

import com.google.common.collect.ImmutableMap;

import java.util.Collection;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Schema describes the table schema of multiple tables
 */
public final class Schema {

    /**
     * name to schema
     */
    private final Map<String, Table> tables;

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
        return tables.entrySet().stream()
            .map(e -> e.getKey() + ": " + e.getValue())
            .collect(Collectors.joining(", ", "{ ", " }"));
    }
}