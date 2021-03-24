package me.ericfu.lightning.source.random;

import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.data.RecordBatchBuilder;
import me.ericfu.lightning.data.RecordBuilder;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.source.SourceReader;

import java.util.Random;

public class RandomSourceReader implements SourceReader {

    private static final int RANDOM_STRING_LENGTH = 20;

    private final RandomSource s;
    private final RecordType recordType;
    private final long end;
    private long current;

    private RecordBatchBuilder builder;
    private Random random;

    public RandomSourceReader(RandomSource s, RecordType recordType, long start, long end) {
        this.s = s;
        this.recordType = recordType;
        this.current = start;
        this.end = end;
    }

    @Override
    public void open() throws DataSourceException {
        this.builder = new RecordBatchBuilder(s.globals.getBatchSize());
        this.random = new Random();
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
        RecordBuilder builder = new RecordBuilder(recordType);
        for (int i = 0; i < recordType.getFieldCount(); i++) {
            final Field field = recordType.getField(i);
            if (s.conf.getAutoIncrementKey().equals(field.getName())) {
                // set auto-increment column to current record number
                builder.set(i, current);
                continue;
            }
            switch (field.getType()) {
            case INT64:
                builder.set(i, (long) random.nextInt());
                break;
            case FLOAT:
                builder.set(i, random.nextFloat());
                break;
            case DOUBLE:
                builder.set(i, random.nextDouble());
                break;
            case STRING:
                builder.set(i, RandomUtils.createRandomAscii(random, RANDOM_STRING_LENGTH));
                break;
            default:
                throw new IllegalStateException("unreachable");
            }
        }
        return builder.build();
    }

    @Override
    public void close() throws DataSourceException {
        // do nothing
    }
}
