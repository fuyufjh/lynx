package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.model.checkpoint.SinkCheckpoint;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.sink.SinkWriter;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;

public class JdbcSinkWriter implements SinkWriter {

    private final JdbcSink s;
    private final JdbcSinkTable table;

    private Connection connection;
    private PreparedStatement ps;

    public JdbcSinkWriter(JdbcSink s, JdbcSinkTable table) {
        this.s = s;
        this.table = table;
    }

    @Override
    public void open() throws DataSinkException {
        String insertTemplate = table.getInsertTemplate();
        try {
            connection = DriverManager.getConnection(s.conf.getUrl(), s.connProps);
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
                    JdbcUtils.setParameter(ps, i + 1, field.getType(), value);
                }
                ps.addBatch();
            }
            ps.executeBatch();
            connection.commit();
        } catch (SQLException ex) {
            throw new DataSinkException(ex);
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
        return null;
    }
}
