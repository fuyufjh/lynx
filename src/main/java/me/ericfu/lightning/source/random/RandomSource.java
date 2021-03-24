package me.ericfu.lightning.source.random;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.data.*;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.schema.Schema;
import me.ericfu.lightning.schema.Table;
import me.ericfu.lightning.source.SchemalessSource;
import me.ericfu.lightning.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Random;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomSource implements SchemalessSource {

    private static final Logger logger = LoggerFactory.getLogger(RandomSource.class);

    private static final int RANDOM_STRING_LENGTH = 20;

    private final GeneralConf globals;
    private final RandomSourceConf conf;

    private Schema schema;

    public RandomSource(GeneralConf globals, RandomSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void provideSchema(Schema schema) {
        this.schema = schema;
    }

    @Override
    public void init() throws DataSourceException {
        // do nothing
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public List<SourceReader> createReaders(Table table) {
        long[] cut = accumulate(split(conf.getRecords(), globals.getThreads()));
        return IntStream.range(0, globals.getThreads()).mapToObj(i -> {
            long start = i > 0 ? cut[i - 1] : 0;
            long end = cut[i];
            return new RandomSourceReader(table.getType(), start, end);
        }).collect(Collectors.toList());
    }

    public class RandomSourceReader implements SourceReader {

        private final RecordType recordType;
        private final long end;
        private long current;

        private RecordBatchBuilder builder;
        private Random random;

        public RandomSourceReader(RecordType recordType, long start, long end) {
            this.recordType = recordType;
            this.current = start;
            this.end = end;
        }

        @Override
        public void open() throws DataSourceException {
            this.builder = new RecordBatchBuilder(globals.getBatchSize());
            this.random = new Random();
        }

        @Override
        public RecordBatch readBatch() throws DataSourceException {
            for (int i = 0; i < globals.getBatchSize() && current < end; i++, current++) {
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
                if (conf.getAutoIncrementKey().equals(field.getName())) {
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
                    builder.set(i, buildRandomString());
                    break;
                default:
                    throw new IllegalStateException("unreachable");
                }
            }
            return builder.build();
        }

        private ByteString buildRandomString() {
            byte[] buf = new byte[RANDOM_STRING_LENGTH];
            for (int i = 0; i < RANDOM_STRING_LENGTH; i++) {
                buf[i] = (byte) (random.nextInt(26) + 'a');
            }
            return new ByteString(buf, StandardCharsets.UTF_8);
        }

        @Override
        public void close() throws DataSourceException {
            // do nothing
        }
    }

    private static long[] split(long x, int n) {
        long[] result = new long[n];
        long r = n - (x % n);
        long d = x / n;
        for (int i = 0; i < n; i++) {
            result[i] = i >= r ? d + 1 : d;
        }
        return result;
    }

    private static long[] accumulate(long[] values) {
        for (int i = 1; i < values.length; i++) {
            values[i] += values[i - 1];
        }
        return values;
    }
}
