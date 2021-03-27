package me.ericfu.lynx.pipeline;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TaskBoard saves all tasks in this Lynx instance
 */
public final class TaskBoard {

    private final Map<String, List<Task>> tableTasks;

    private TaskBoard(Map<String, List<Task>> tableTasks) {
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

        public TaskBoard build() {
            return new TaskBoard(tableTasks);
        }
    }
}
