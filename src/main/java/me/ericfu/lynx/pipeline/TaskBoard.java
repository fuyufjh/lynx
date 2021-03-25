package me.ericfu.lynx.pipeline;

import com.google.common.collect.Iterables;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * TaskBoard saves all pipelines in this Lynx instance
 */
public final class TaskBoard {

    private final Map<String, List<Pipeline>> schemaPipelines;

    private TaskBoard(Map<String, List<Pipeline>> schemaPipelines) {
        this.schemaPipelines = schemaPipelines;
    }

    public Map<String, List<Pipeline>> getSchemaPipelines() {
        return schemaPipelines;
    }

    public Iterable<Pipeline> getPipelines() {
        return Iterables.concat(schemaPipelines.values());
    }

    public static class Builder {

        private final Map<String, List<Pipeline>> schemaPipelines = new HashMap<>();
        private String currentTable;

        public void setCurrentTable(String table) {
            assert !schemaPipelines.containsKey(table);
            this.currentTable = table;
            schemaPipelines.put(table, new ArrayList<>());
        }

        public void addPipeline(Pipeline pipeline) {
            assert schemaPipelines.containsKey(currentTable);
            schemaPipelines.get(currentTable).add(pipeline);
        }

        public TaskBoard build() {
            return new TaskBoard(schemaPipelines);
        }
    }
}
