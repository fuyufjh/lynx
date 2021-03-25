package me.ericfu.lynx.source.random;

import me.ericfu.lynx.conf.GeneralConf;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.schema.RecordTypeBuilder;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.SchemaBuilder;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.source.Source;
import me.ericfu.lynx.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(RandomSource.class);

    final GeneralConf globals;
    final RandomSourceConf conf;

    Schema schema;

    public RandomSource(GeneralConf globals, RandomSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSourceException {
        this.schema = loadPredefinedSchema();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    /**
     * Load schema defined in config file
     */
    private Schema loadPredefinedSchema() {
        SchemaBuilder schema = new SchemaBuilder();
        conf.getColumns().forEach((table, columns) -> {
            RecordTypeBuilder type = new RecordTypeBuilder();
            columns.forEach(rule -> {
                type.addField(rule.getName(), rule.getType());
            });
            schema.addTable(new Table(table, type.build()));
        });
        return schema.build();
    }

    @Override
    public List<SourceReader> createReaders(Table table) {
        long[] cut = accumulate(split(conf.getRecords(), globals.getThreads()));
        return IntStream.range(0, globals.getThreads()).mapToObj(i -> {
            long start = i > 0 ? cut[i - 1] : 0;
            long end = cut[i];
            return new RandomSourceReader(this, table, start, end);
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
