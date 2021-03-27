package me.ericfu.lynx.pipeline;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Pipeline is a collection of tasks, belonging to one or more tables. Currently a Lynx
 * instance contains only one pipeline.
 */
public final class Pipeline {

    private final Map<String, List<Task>> tableTasks;

    private Pipeline(Map<String, List<Task>> tableTasks) {
        this.tableTasks = tableTasks;
    }

    public Map<String, List<Task>> getTableTasks() {
        return tableTasks;
    }

    public Iterable<Task> getTasks() {
        return Iterables.concat(tableTasks.values());
    }

    public static class Builder {

        private final Map<String, List<Task>> tableTasks = new HashMap<>();
        private String currentTable;

        public void setCurrentTable(String table) {
            assert !tableTasks.containsKey(table);
            this.currentTable = table;
            tableTasks.put(table, new ArrayList<>());
        }

        public void addTask(Task task) {
            assert tableTasks.containsKey(currentTable);
            tableTasks.get(currentTable).add(task);
        }

        public Pipeline build() {
            return new Pipeline(tableTasks);
        }
    }
}
