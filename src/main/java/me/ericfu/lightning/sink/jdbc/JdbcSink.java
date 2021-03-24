package me.ericfu.lightning.sink.jdbc;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.*;
import me.ericfu.lightning.sink.Sink;
import me.ericfu.lightning.sink.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.HashMap;
import java.util.Map;
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

    public JdbcSink(GeneralConf globals, JdbcSinkConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSinkException {
        // Fetch schema via JDBC metadata interface
        Map<String, RecordTypeBuilder> recordTypeBuilders = new HashMap<>();
        try {
            Connection connection = DriverManager.getConnection(conf.getUrl(), conf.getUser(), conf.getPassword());

            // Extract schema from target table
            DatabaseMetaData meta = connection.getMetaData();
            try (ResultSet rs = meta.getColumns(null, null, conf.getTable(), null)) {
                while (rs.next()) {
                    String tableName = rs.getString("TABLE_NAME");
                    String columnName = rs.getString("COLUMN_NAME");
                    int jdbcType = rs.getInt("DATA_TYPE");
                    BasicType dataType = JdbcUtils.convertJdbcType(jdbcType);

                    RecordTypeBuilder r = recordTypeBuilders.computeIfAbsent(tableName, t -> new RecordTypeBuilder());
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
            .collect(Collectors.toMap(Table::getName, t -> buildInsertTemplate(t.getType())));
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public SinkWriter createWriter(Table table) {
        return new JdbcSinkWriter(this, table);
    }

    private String buildInsertTemplate(RecordType type) {
        String fieldList = type.getFields().stream().map(Field::getName)
            .collect(Collectors.joining(",", "(", ")"));
        String valueList = type.getFields().stream().map(x -> "?")
            .collect(Collectors.joining(",", "(", ")"));
        return "INSERT INTO " + conf.getTable() + fieldList + " VALUES " + valueList;
    }
}
