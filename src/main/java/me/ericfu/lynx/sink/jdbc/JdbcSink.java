package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.SchemaBuilder;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.sink.Sink;
import me.ericfu.lynx.sink.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

public class JdbcSink implements Sink {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSink.class);

    final GeneralConf globals;
    final JdbcSinkConf conf;

    Schema schema;

    /**
     * Insert statement templates for each table
     */
    Map<String, String> insertTemplates;

    Properties connProps;

    public JdbcSink(GeneralConf globals, JdbcSinkConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSinkException {
        // build connection properties
        connProps = JdbcUtils.buildConnProps(conf.getUser(), conf.getPassword(), conf.getProperties());

        // Fetch schema via JDBC metadata interface
        Map<String, StructType.Builder> recordTypeBuilders = new HashMap<>();
        try (Connection connection = DriverManager.getConnection(conf.getUrl(), connProps)) {
            // Extract schema from target table
            DatabaseMetaData meta = connection.getMetaData();
            try (ResultSet rs = meta.getColumns(null, null, "%", null)) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    int jdbcType = rs.getInt("DATA_TYPE");
                    BasicType dataType = JdbcUtils.convertJdbcType(jdbcType);

                    StructType.Builder r = recordTypeBuilders.computeIfAbsent(tableName, t -> new StructType.Builder());
                    r.addField(columnName, dataType);
                }
            }
        } catch (SQLException ex) {
            throw new DataSinkException("cannot fetch metadata", ex);
        }

        SchemaBuilder schemaBuilder = new SchemaBuilder();
        recordTypeBuilders.forEach((name, builder) -> {
            Table table = new Table(name, builder.build());
            schemaBuilder.addTable(table);
        });
        schema = schemaBuilder.build();

        // build insert statement template
        insertTemplates = schema.getTables().stream()
            .collect(Collectors.toMap(Table::getName, this::buildInsertTemplate));
    }

    @Override
    public Schema getSchema(Schema source) {
        return schema;
    }

    @Override
    public SinkWriter createWriter(Table table) {
        return new JdbcSinkWriter(this, table);
    }

    private String buildInsertTemplate(Table table) {
        String fieldList = table.getType().getFields().stream().map(Field::getName)
            .collect(Collectors.joining(",", "(", ")"));
        String valueList = table.getType().getFields().stream().map(x -> "?")
            .collect(Collectors.joining(",", "(", ")"));
        return "INSERT IGNORE INTO " + table.getName() + fieldList + " VALUES " + valueList;
    }
}
