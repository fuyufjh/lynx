package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.data.ByteArray;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.model.checkpoint.SinkCheckpoint;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.sink.SinkWriter;

import java.sql.*;

public class JdbcSinkWriter implements SinkWriter {

    private final JdbcSink s;
    private final Table table;

    private Connection connection;
    private PreparedStatement ps;

    public JdbcSinkWriter(JdbcSink s, Table table) {
        this.s = s;
        this.table = table;
    }

    @Override
    public void open() throws DataSinkException {
        String insertTemplate = s.insertTemplates.get(table.getName());
        try {
            connection = DriverManager.getConnection(s.conf.getUrl(), s.conf.getUser(), s.conf.getPassword());
            connection.setAutoCommit(false);
            ps = connection.prepareStatement(insertTemplate);
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
        }
    }

    @Override
    public void open(SinkCheckpoint checkpoint) throws DataSinkException {
        open();
    }

    @Override
    public void writeBatch(RecordBatch batch) throws DataSinkException {
        try {
            for (Record record : batch) {
                for (int i = 0; i < table.getType().getFieldCount(); i++) {
                    final Object value = record.getValue(i);
                    final Field field = table.getType().getField(i);
                    setFieldValue(i + 1, field, value);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
        }
    }

    private void setFieldValue(int i, Field field, Object value) throws SQLException {
        switch (field.getType()) {
        case BOOLEAN:
            if (value != null) {
                ps.setBoolean(i, (boolean) value);
            } else {
                ps.setNull(i, Types.BOOLEAN);
            }
            break;
        case INT:
            if (value != null) {
                ps.setInt(i, (int) value);
            } else {
                ps.setNull(i, Types.INTEGER);
            }
            break;
        case LONG:
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
                ps.setString(i, (String) value);
            } else {
                ps.setNull(i, Types.VARCHAR);
            }
            break;
        case BINARY:
            if (value != null) {
                ps.setBytes(i, ((ByteArray) value).getBytes());
            } else {
                ps.setNull(i, Types.BINARY);
            }
            break;
        default:
            throw new AssertionError();
        }
    }

    @Override
    public void close() throws DataSinkException {
        try {
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
        }
    }

    @Override
    public SinkCheckpoint checkpoint() {
        // Returns an empty checkpoint because of nothing to save
        return new SinkCheckpoint() {
        };
    }
}
