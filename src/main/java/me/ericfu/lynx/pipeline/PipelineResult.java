package me.ericfu.lynx.pipeline;

/**
 * Result summary of a pipeline
 */
public class PipelineResult {

    private final long records;

    public PipelineResult(long records) {
        this.records = records;
    }

    public long getRecords() {
        return records;
    }
}
