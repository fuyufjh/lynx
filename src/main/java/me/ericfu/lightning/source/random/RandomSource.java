package me.ericfu.lightning.source.random;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.data.ByteString;
import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.data.RecordBatchBuilder;
import me.ericfu.lightning.data.RecordBuilder;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.source.SchemalessSource;
import me.ericfu.lightning.source.SourceReader;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class RandomSource implements SchemalessSource {

    private static final int RANDOM_STRING_LENGTH = 20;

    private final GeneralConf globals;
    private final RandomSourceConf conf;

    private RecordType schema;

    public RandomSource(GeneralConf globals, RandomSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void provideSchema(RecordType schema) {
        this.schema = schema;
    }

    @Override
    public void init() throws DataSourceException {
        // do nothing
    }

    @Override
    public RecordType getSchema() {
        return schema;
    }

    @Override
    public SourceReader createReader(int partNo) {
        int[] splits = split(conf.getRecords(), globals.getThreads());
        return new RandomSourceReader(splits[partNo]);
    }

    public class RandomSourceReader implements SourceReader {

        private final int limit;
        private int count;

        private RecordBatchBuilder builder;
        private Random random;

        public RandomSourceReader(int limit) {
            this.limit = limit;
        }

        @Override
        public void open() throws DataSourceException {
            this.builder = new RecordBatchBuilder(globals.getBatchSize());
            this.random = new Random();
        }

        @Override
        public RecordBatch readBatch() throws DataSourceException {
            for (int i = 0; i < globals.getBatchSize() && count < limit; i++, count++) {
                builder.addRow(buildRandomRecord());
            }

            if (builder.size() > 0) {
                return builder.buildAndReset();
            } else {
                return null;
            }
        }

        private Record buildRandomRecord() {
            RecordBuilder builder = new RecordBuilder(schema);
            for (int i = 0; i < schema.getFieldCount(); i++) {
                switch (schema.getField(i).getType()) {
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

    private static int[] split(int x, int n) {
        int[] result = new int[n];
        int r = n - (x % n);
        int d = x / n;
        for (int i = 0; i < n; i++) {
            result[i] = i >= r ? d + 1 : d;
        }
        return result;
    }
}
