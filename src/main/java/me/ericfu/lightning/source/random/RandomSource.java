package me.ericfu.lightning.source.random;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.Schema;
import me.ericfu.lightning.schema.Table;
import me.ericfu.lightning.source.SchemalessSource;
import me.ericfu.lightning.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomSource implements SchemalessSource {

    private static final Logger logger = LoggerFactory.getLogger(RandomSource.class);

    final GeneralConf globals;
    final RandomSourceConf conf;

    Schema schema;

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
            return new RandomSourceReader(this, table.getType(), start, end);
        }).collect(Collectors.toList());
    }

    /**
     * Split number X into N parts such that difference between the smallest and the largest part is minimum
     * <p>
     * For example, given X = 10 and N = 3, the output is [3, 3, 4]
     *
     * @param x total number X
     * @param n number of parts N
     * @return the best split results
     */
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
