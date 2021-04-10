package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.sink.Sink;
import me.ericfu.lynx.sink.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class JdbcSink implements Sink {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSink.class);

    final GeneralConf globals;
    final JdbcSinkConf conf;

    Schema schema;

    Properties connProps;

    public JdbcSink(GeneralConf globals, JdbcSinkConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init(Schema sourceSchema) throws DataSinkException {
        // build connection properties
        connProps = JdbcUtils.buildConnProps(conf.getUser(), conf.getPassword(), conf.getProperties());

        Schema.Builder schemaBuilder = new Schema.Builder();
        try (Connection conn = DriverManager.getConnection(conf.getUrl(), connProps)) {
            for (Table sourceTable : sourceSchema.getTables()) {
                // Fetch schema via JDBC metadata interface
                Map<String, BasicType> columnTypes = JdbcUtils.fetchColumnTypes(conn.getMetaData(),
                    conn.getCatalog(), conn.getSchema(), sourceTable.getName());

                Iterable<String> columns;
                if (sourceTable.getType() instanceof StructType) {
                    columns = ((StructType) sourceTable.getType()).getFields().stream()
                        .map(Field::getName)
                        .collect(Collectors.toList());
                } else {
                    // Export all the columns if not specified by source
                    columns = columnTypes.keySet();
                }

                JdbcSinkTable table = buildTable(sourceTable.getName(), columns, columnTypes);
                schemaBuilder.addTable(table);
            }
        } catch (SQLException ex) {
            throw new DataSinkException("cannot fetch metadata", ex);
        }

        schema = schemaBuilder.build();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public SinkWriter createWriter(Table table, int no) {
        assert table instanceof JdbcSinkTable;
        return new JdbcSinkWriter(this, (JdbcSinkTable) table);
    }

    private String buildInsertTemplate(String table, List<Field> fields) {
        String fieldList = fields.stream().map(Field::getName).map(this::quoteIdentifier)
            .collect(Collectors.joining(",", "(", ")"));
        String valueList = fields.stream().map(x -> "?")
            .collect(Collectors.joining(",", "(", ")"));
        return "INSERT INTO " + quoteIdentifier(table) + fieldList + " VALUES " + valueList;
    }

    private JdbcSinkTable buildTable(String tableName, Iterable<String> columns, Map<String, BasicType> columnTypes)
        throws DataSinkException {
        StructType.Builder typeBuilder = new StructType.Builder();
        for (String column : columns) {
            final BasicType type = columnTypes.get(column);
            if (type == null) {
                throw new DataSinkException("column '" + column + "' not found in table '" + tableName + "'");
            }
            typeBuilder.addField(column, type);
        }
        StructType type = typeBuilder.build();
        String insertTemplate = buildInsertTemplate(tableName, type.getFields());
        return new JdbcSinkTable(tableName, type, insertTemplate);
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
}
