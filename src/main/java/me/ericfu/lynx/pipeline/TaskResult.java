package me.ericfu.lynx.pipeline;

/**
 * Result summary of a task
 */
public class TaskResult {

    private final long records;

    public TaskResult(long records) {
        this.records = records;
    }

    public long getRecords() {
        return records;
    }
}
