package me.ericfu.lynx.source.jdbc;

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Range;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.SchemaBuilder;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.sink.jdbc.JdbcUtils;
import me.ericfu.lynx.source.Source;
import me.ericfu.lynx.source.SourceReader;
import me.ericfu.lynx.source.jdbc.JdbcSourceConf.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.*;
import java.util.stream.Collectors;

public class JdbcSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSource.class);

    final GeneralConf globals;
    final JdbcSourceConf conf;

    Schema schema;
    Properties connProps;

    Map<String, List<TableSplit>> tableSplits;

    public JdbcSource(GeneralConf globals, JdbcSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSourceException {
        // build connection properties
        connProps = JdbcUtils.buildConnProps(conf.getUser(), conf.getPassword(), conf.getProperties());

        tableSplits = new LinkedHashMap<>();

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        try (Connection connection = DriverManager.getConnection(conf.getUrl(), connProps)) {
            for (Map.Entry<String, TableDesc> e : conf.getTables().entrySet()) {
                final String tableName = e.getKey();
                final TableDesc desc = e.getValue() == null ? new TableDesc() : e.getValue();

                // Fetch schema via JDBC metadata interface
                Map<String, BasicType> columnTypes = fetchColumnTypes(connection.getMetaData(), connection.getSchema(), tableName);

                // Build table schema
                Table table = buildTable(tableName, desc, columnTypes);
                schemaBuilder.addTable(table);

                // Split table into multiple parts if possible
                List<TableSplit> tableSplits = buildTableSplits(table, columnTypes, connection);
                this.tableSplits.put(table.getName(), tableSplits);
            }
        } catch (SQLException ex) {
            throw new DataSourceException("cannot fetch metadata", ex);
        }

        schema = schemaBuilder.build();
    }

    private Map<String, BasicType> fetchColumnTypes(DatabaseMetaData meta, String schema, String table)
        throws SQLException, DataSourceException {
        // Collect column types
        Map<String, BasicType> columnTypes = new LinkedHashMap<>();
        try (ResultSet rs = meta.getColumns(null, null, table, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int jdbcType = rs.getInt("DATA_TYPE");
                columnTypes.put(columnName, JdbcUtils.convertJdbcType(jdbcType));
            }
        }

        if (columnTypes.isEmpty()) {
            throw new DataSourceException("table '" + table + "' not found");
        }
        return columnTypes;
    }

    private Table buildTable(String tableName, TableDesc desc, Map<String, BasicType> columnTypes)
        throws DataSourceException {
        // Export all the columns if columns not specified in conf
        Iterable<String> columns = desc.getColumns() != null ? desc.getColumns() : columnTypes.keySet();

        StructType.Builder rb = new StructType.Builder();
        for (String column : columns) {
            final BasicType type = columnTypes.get(column);
            if (type == null) {
                throw new DataSourceException("column '" + column + "' not found in table '" + tableName + "'");
            }
            rb.addField(column, type);
        }

        return new Table(tableName, rb.build());
    }

    /**
     * Build table splits according to table schema and user conf
     */
    private List<TableSplit> buildTableSplits(Table table, Map<String, BasicType> columnTypes, Connection connection)
        throws SQLException {

        /*
         * Automatically choose a split key, which must satisfies:
         * 1) be and be the only column in primary key
         * 2) column type is integer number (INT or LONG)
         */
        String splitKey;
        try (ResultSet rs = connection.getMetaData().getPrimaryKeys(null, null, table.getName())) {
            List<String> primaryKeyColumns = new ArrayList<>();
            while (rs.next()) {
                primaryKeyColumns.add(rs.getString("COLUMN_NAME"));
            }
            if (primaryKeyColumns.size() != 1) {
                splitKey = null;
                if (primaryKeyColumns.size() == 0) {
                    logger.warn("Table '{}' will be read with single thread since it does not contain a primary key", table.getName());
                } else {
                    logger.warn("Table '{}' will be read with single thread since it contains a composite primary key", table.getName());
                }
            } else {
                BasicType type = columnTypes.get(primaryKeyColumns.get(0));
                if (type == BasicType.INT || type == BasicType.LONG) {
                    splitKey = primaryKeyColumns.get(0);
                    logger.info("Choose column '{}' as default split key for table '{}'", splitKey, table.getName());
                } else {
                    splitKey = null;
                    logger.warn("Table '{}' will be read with single thread since it's primary key '{}' is not integer", table.getName(), primaryKeyColumns.get(0));
                }
            }
        }

        if (splitKey == null) {
            return Collections.singletonList(new TableSplit(table));
        } else {
            return buildTableSplits(connection, table, splitKey);
        }
    }

    private List<TableSplit> buildTableSplits(Connection connection, Table table, String splitKey)
        throws SQLException {

        // Fetch min/max value of split key
        Long min = null, max = null;
        try (Statement statement = connection.createStatement()) {
            String query = String.format("SELECT MIN(%s), MAX(%s) FROM %s",
                quoteIdentifier(splitKey), quoteIdentifier(splitKey), quoteIdentifier(table.getName()));
            try (ResultSet rs = statement.executeQuery(query)) {
                while (rs.next()) {
                    min = rs.getLong(1);
                    if (rs.wasNull()) {
                        min = null;
                    }
                    max = rs.getLong(2);
                    if (rs.wasNull()) {
                        max = null;
                    }
                }
            }
        }

        if (min == null || max == null) {
            logger.warn("Table '{}' will be read with single thread since since it is empty or the split key contains NULL value", table.getName());
            return Collections.singletonList(new TableSplit(table));
        }

        // By default generate parts according to number of threads
        List<Range<Long>> ranges = buildSplitRanges(min, max, globals.getThreads());

        logger.info("Table '{}' will be split into {} parts and read in parallel", table.getName(), ranges.size());
        return ranges.stream()
            .map(r -> new TableSplit(table, splitKey, r))
            .collect(Collectors.toList());
    }

    /**
     * Split the value range [min, max] to a bunch of splits. e.g.
     * <pre>
     * * input = [0, 10], parts = 3
     * * quantiles = [3, 6, 10]
     * * output = (-inf, 3], (3, 6], (6, +inf)
     * </pre>
     */
    @VisibleForTesting
    static List<Range<Long>> buildSplitRanges(long min, long max, int parts) {
        List<Range<Long>> ranges = new ArrayList<>(parts);
        long[] quantiles = accumulate(split(max - min, parts));
        for (int i = 0; i < parts; i++) {
            Range<Long> range;
            if (i == 0) {
                range = Range.atMost(quantiles[0]);
            } else if (i == parts - 1) {
                range = Range.greaterThan(quantiles[parts - 2]);
            } else {
                range = Range.openClosed(quantiles[i - 1], quantiles[i]); // (begin, end]
            }
            ranges.add(range);
        }
        return ranges;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public List<SourceReader> createReaders(Table table) {
        return tableSplits.get(table.getName()).stream()
            .map(split -> new JdbcSourceReader(this, split))
            .collect(Collectors.toList());
    }

    /*
     * Quote identifier, for example, with quotation mark (") or backquote (`), etc.
     */
    String quoteIdentifier(String identifier) {
        char mark;
        switch (conf.getQuoteIdentifier()) {
        case NO_QUOTE:
            return identifier;
        case DOUBLE:
            mark = '"';
            break;
        case SINGLE:
            mark = '\'';
            break;
        case BACKQUOTE:
            mark = '`';
            break;
        default:
            throw new AssertionError();
        }
        // Escape quotation mark in identifier name with backlash
        return mark + identifier.replace(String.valueOf(mark), "\\" + mark) + mark;
    }

    static class TableSplit {

        final Table table;
        final String splitKey;
        final Range<Long> splitRange;

        /**
         * Constructor for non-splittable table
         */
        TableSplit(Table table) {
            this(table, null, null);
        }

        /**
         * Constructor for a singe split of a splittable table
         */
        TableSplit(Table table, String splitKey, Range<Long> splitRange) {
            this.table = table;
            this.splitKey = splitKey;
            this.splitRange = splitRange;
        }

        boolean isSplittable() {
            return splitKey != null;
        }
    }

    /**
     * Split number X into N parts such that difference between the smallest and the largest part is minimum
     * <p>
     * For example, given X = 10 and N = 3, the output is [3, 3, 4]
     * <p>
     * TODO: duplicate in RandomSource
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
