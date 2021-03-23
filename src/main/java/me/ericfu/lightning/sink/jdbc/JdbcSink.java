package me.ericfu.lightning.sink.jdbc;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.data.Batch;
import me.ericfu.lightning.data.ByteString;
import me.ericfu.lightning.data.Row;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.BasicType;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.schema.RecordTypeBuilder;
import me.ericfu.lightning.sink.SchemaSink;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Types;
import java.util.stream.Collectors;

public class JdbcSink implements SchemaSink {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSink.class);

    private final GeneralConf globals;
    private final JdbcSinkConf conf;

    private RecordType schema;

    private Connection connection;
    private PreparedStatement ps;

    public JdbcSink(GeneralConf globals, JdbcSinkConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void open() throws DataSinkException {
        try {
            connection = DriverManager.getConnection(conf.getUrl(), conf.getUser(), conf.getPassword());
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
        }

        // Extract schema from target table
        RecordTypeBuilder schemaBuilder = new RecordTypeBuilder();
        try {
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

        // prepare insert statement
        String insertTemplate = buildInsertTemplate();
        try {
            ps = connection.prepareStatement(insertTemplate);
        } catch (SQLException ex) {
            throw new DataSinkException("prepare failed", ex);
        }
    }

    @Override
    public void writeBatch(Batch batch) throws DataSinkException {
        try {
            for (Row row : batch) {
                for (int i = 0; i < schema.getFieldCount(); i++) {
                    final Object value = row.getValue(i);
                    final Field field = schema.getField(i);
                    setFieldValue(i + 1, field, value);
                }
                ps.addBatch();
            }
            ps.executeUpdate();
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
        }
    }

    private void setFieldValue(int i, Field field, Object value) throws SQLException {
        switch (field.getType()) {
        case INT64:
            if (value != null) {
                ps.setLong(i, (long) value);
            } else {
                ps.setNull(i, Types.BIGINT);
            }
            break;
        case FLOAT:
            if (value != null) {
                ps.setFloat(i, (float) value);
            } else {
                ps.setNull(i, Types.FLOAT);
            }
            break;
        case DOUBLE:
            if (value != null) {
                ps.setDouble(i, (double) value);
            } else {
                ps.setNull(i, Types.DOUBLE);
            }
            break;
        case STRING:
            if (value != null) {
                ps.setBinaryStream(i, ((ByteString) value).getBinaryStream());
            } else {
                ps.setNull(i, Types.BINARY);
            }
            break;
        }
    }

    @Override
    public void close() throws DataSinkException {
        try {
            connection.close();
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
        }
    }

    @Override
    public RecordType getSchema() {
        return schema;
    }

    private String buildInsertTemplate() {
        String fieldList = schema.getFields().stream().map(Field::getName)
            .collect(Collectors.joining(",", "(", ")"));
        String valueList = schema.getFields().stream().map(x -> "?")
            .collect(Collectors.joining(",", "(", ")"));
        return "INSERT OR IGNORE INTO " + conf.getTable() + fieldList + " VALUES " + valueList;
    }
}
