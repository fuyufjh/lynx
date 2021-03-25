package me.ericfu.lynx.source.random;

import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.data.RecordBatchBuilder;
import me.ericfu.lynx.data.RecordBuilder;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.source.SourceReader;

import java.util.Random;

public class RandomSourceReader implements SourceReader {

    private static final int RANDOM_STRING_LENGTH = 20;

    private final RandomSource s;
    private final Table table;
    private final long end;
    private long current;

    private RecordBatchBuilder builder;
    private Random random;
    private RandomGenerator[] generators;

    public RandomSourceReader(RandomSource s, Table table, long start, long end) {
        this.s = s;
        this.table = table;
        this.current = start;
        this.end = end;
    }

    @Override
    public void open() throws DataSourceException {
        this.builder = new RecordBatchBuilder(s.globals.getBatchSize());
        this.random = new Random();

        this.generators = new RandomGenerator[table.getType().getFieldCount()];
        for (int i = 0; i < table.getType().getFieldCount(); i++) {
            Field field = table.getType().getField(i);

            // Find the matched column rule and read the rule code
            String code = null;
            if (s.conf.getColumns() != null) {
                for (RandomSourceConf.RandomRule r : s.conf.getColumns().get(table.getName())) {
                    if (r.getName().equals(field.getName())) {
                        if (r.getRule() != null) {
                            code = r.getRule();
                            break;
                        }
                    }
                }
            }

            if (code == null) {
                generators[i] = createDefaultGenerator(field);
            } else {
                generators[i] = new RandomGeneratorCompiler().compile(code);
            }
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
            builder.set(i, generators[i].generate(current, random));
        }
        return builder.build();
    }

    private static RandomGenerator createDefaultGenerator(Field field) {
        switch (field.getType()) {
        case INT64:
            // TODO: INT32/INT64
            return (i, r) -> (long) r.nextInt();
        case FLOAT:
            return (i, r) -> r.nextFloat();
        case DOUBLE:
            return (i, r) -> r.nextDouble();
        case STRING:
            // TODO: save precision/scale in Field and use it as string length
            return (i, r) -> RandomUtils.createRandomAscii(r, RANDOM_STRING_LENGTH);
        default:
            throw new AssertionError();
        }
    }

    @Override
    public void close() throws DataSourceException {
        // do nothing
    }
}
