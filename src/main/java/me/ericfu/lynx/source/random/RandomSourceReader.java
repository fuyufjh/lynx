package me.ericfu.lynx.source.random;

import lombok.Data;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.data.RecordBatchBuilder;
import me.ericfu.lynx.data.RecordBuilder;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.checkpoint.SourceCheckpoint;
import me.ericfu.lynx.source.SourceReader;

import java.util.Random;

class RandomSourceReader implements SourceReader {

    private final RandomSource s;
    private final RandomSourceTable table;
    private final long end;
    private long current;

    private RecordBatchBuilder builder;
    private Random random;

    public RandomSourceReader(RandomSource s, RandomSourceTable table, long start, long end) {
        this.s = s;
        this.table = table;
        this.current = start;
        this.end = end;
    }

    @Override
    public void open(SourceCheckpoint checkpoint) throws DataSourceException {
        this.builder = new RecordBatchBuilder(s.globals.getBatchSize());
        this.random = new Random();

        if (checkpoint != null) {
            Checkpoint cp = (Checkpoint) checkpoint;
            this.current = cp.nextRowNum;
        }
    }

    @Override
    public RecordBatch readBatch() throws DataSourceException {
        for (int i = 0; i < s.globals.getBatchSize() && current < end; i++, current++) {
            builder.addRow(buildRandomRecord());
        }

        if (builder.size() > 0) {
            return builder.buildAndReset();
        } else {
            return null;
        }
    }

    private Record buildRandomRecord() {
        RecordBuilder builder = new RecordBuilder(table.getType());
        for (int i = 0; i < table.getType().getFieldCount(); i++) {
            builder.set(i, table.getGenerator(i).generate(current + 1, random));
        }
        return builder.build();
    }


    @Override
    public void close() throws DataSourceException {
        // do nothing
    }

    @Override
    public SourceCheckpoint checkpoint() {
        Checkpoint cp = new Checkpoint();
        cp.setNextRowNum(current);
        return cp;
    }

    @Data
    public static class Checkpoint implements SourceCheckpoint {
        private long nextRowNum;
    }
}
