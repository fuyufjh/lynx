package me.ericfu.lightning.sink.jdbc;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.BasicType;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.schema.RecordTypeBuilder;
import me.ericfu.lightning.sink.Sink;
import me.ericfu.lightning.sink.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.stream.Collectors;

public class JdbcSink implements Sink {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSink.class);

    final GeneralConf globals;
    final JdbcSinkConf conf;

    RecordType schema;
    String insertTemplate;

    public JdbcSink(GeneralConf globals, JdbcSinkConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSinkException {
        // Fetch schema via JDBC metadata interface
        RecordTypeBuilder schemaBuilder = new RecordTypeBuilder();
        try {
            Connection connection = DriverManager.getConnection(conf.getUrl(), conf.getUser(), conf.getPassword());

            // Extract schema from target table
            DatabaseMetaData dbmd = connection.getMetaData();
            try (ResultSet rs = dbmd.getColumns(null, null, conf.getTable(), null)) {
                while (rs.next()) {
                    String columnName = rs.getString("COLUMN_NAME");
                    int jdbcType = rs.getInt("DATA_TYPE");
                    BasicType dataType = JdbcUtils.convertJdbcType(jdbcType);
                    schemaBuilder.addField(columnName, dataType);
                }
            }
        } catch (SQLException ex) {
            throw new DataSinkException("cannot fetch metadata", ex);
        }
        schema = schemaBuilder.build();

        // build insert statement template
        insertTemplate = buildInsertTemplate();
    }

    @Override
    public RecordType getSchema() {
        return schema;
    }

    @Override
    public SinkWriter createWriter(int partNo) {
        return new JdbcSinkWriter(this);
    }

    private String buildInsertTemplate() {
        String fieldList = schema.getFields().stream().map(Field::getName)
            .collect(Collectors.joining(",", "(", ")"));
        String valueList = schema.getFields().stream().map(x -> "?")
            .collect(Collectors.joining(",", "(", ")"));
        return "INSERT INTO " + conf.getTable() + fieldList + " VALUES " + valueList;
    }
}
