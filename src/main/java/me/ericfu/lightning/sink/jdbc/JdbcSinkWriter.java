package me.ericfu.lightning.sink.jdbc;

import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.sink.SinkWriter;

import java.sql.*;

public class JdbcSinkWriter implements SinkWriter {

    private final JdbcSink s;

    private Connection connection;
    private PreparedStatement ps;

    public JdbcSinkWriter(JdbcSink s) {
        this.s = s;
    }

    @Override
    public void open() throws DataSinkException {
        try {
            connection = DriverManager.getConnection(s.conf.getUrl(), s.conf.getUser(), s.conf.getPassword());
            ps = connection.prepareStatement(s.insertTemplate);
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
        }
    }

    @Override
    public void writeBatch(RecordBatch batch) throws DataSinkException {
        try {
            for (Record record : batch) {
                for (int i = 0; i < s.schema.getFieldCount(); i++) {
                    final Object value = record.getValue(i);
                    final Field field = s.schema.getField(i);
                    setFieldValue(i + 1, field, value);
                }
                ps.addBatch();
            }
            ps.executeBatch();
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
                // TODO: improve performance and split binary/string
                ps.setString(i, value.toString());
            } else {
                ps.setNull(i, Types.VARCHAR);
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
}
