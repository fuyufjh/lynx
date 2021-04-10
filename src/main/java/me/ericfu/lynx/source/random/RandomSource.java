package me.ericfu.lynx.source.random;

import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.source.Source;
import me.ericfu.lynx.source.SourceReader;
import me.ericfu.lynx.source.SourceUtils;
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
        Schema.Builder schema = new Schema.Builder();
        conf.getTables().forEach((table, columns) -> {
            StructType.Builder type = new StructType.Builder();
            columns.forEach(rule -> {
                type.addField(rule.getName(), rule.getType());
            });
            schema.addTable(new RandomSourceTable(table, type.build()));
        });
        return schema.build();
    }

    @Override
    public List<SourceReader> createReaders(Table table) {
        assert table instanceof RandomSourceTable;
        long[] cut = SourceUtils.quantiles(conf.getRecords(), globals.getThreads());
        return IntStream.range(0, globals.getThreads()).mapToObj(i -> {
            long start = i > 0 ? cut[i - 1] : 0;
            long end = cut[i];
            return new RandomSourceReader(this, (RandomSourceTable) table, start, end);
        }).collect(Collectors.toList());
    }
}
