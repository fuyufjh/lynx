package me.ericfu.lynx.source.random;

import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.source.Source;
import me.ericfu.lynx.source.SourceReader;
import me.ericfu.lynx.source.SourceUtils;
import me.ericfu.lynx.source.random.RandomSourceConf.ColumnSpec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class RandomSource implements Source {

    private static final int RAND_STRING_LENGTH = 10;

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
        Schema.Builder schema = new Schema.Builder();
        for (Map.Entry<String, List<ColumnSpec>> e : conf.getTables().entrySet()) {
            final String table = e.getKey();
            final List<ColumnSpec> columns = e.getValue();

            StructType.Builder type = new StructType.Builder();
            RandomGenerator[] generators = new RandomGenerator[columns.size()];
            for (int i = 0; i < columns.size(); i++) {
                final ColumnSpec spec = columns.get(i);
                type.addField(spec.getName(), spec.getType());
                if (spec.getRule() == null) {
                    generators[i] = createDefaultGenerator(spec.getType());
                } else {
                    generators[i] = new RandomGeneratorCompiler().compile(spec.getRule(), spec.getType().getClazz());
                }
            }
            schema.addTable(new RandomSourceTable(table, type.build(), generators));
        }
        this.schema = schema.build();
    }

    @Override
    public Schema getSchema() {
        return schema;
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

    private static RandomGenerator createDefaultGenerator(BasicType type) {
        switch (type) {
        case BOOLEAN:
            return (i, r) -> r.nextBoolean();
        case INT:
            return (i, r) -> r.nextInt();
        case LONG:
            return (i, r) -> r.nextLong();
        case FLOAT:
            return (i, r) -> r.nextFloat();
        case DOUBLE:
            return (i, r) -> r.nextDouble();
        case STRING:
            return (i, r) -> RandomUtils.randomAsciiString(r, RAND_STRING_LENGTH);
        case BINARY:
            return (i, r) -> RandomUtils.randomBinary(r, RAND_STRING_LENGTH);
        default:
            throw new AssertionError();
        }
    }
}
