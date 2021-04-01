package me.ericfu.lynx.source.jdbc;

import com.google.common.collect.BoundType;
import com.google.common.collect.ImmutableList;
import lombok.Data;
import me.ericfu.lynx.data.ByteArray;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.data.RecordBatchBuilder;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.checkpoint.SourceCheckpoint;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.RecordType;
import me.ericfu.lynx.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class JdbcSourceReader implements SourceReader {

    private static final Logger logger = LoggerFactory.getLogger(JdbcSourceReader.class);

    private final JdbcSource s;
    private final JdbcSource.TableSplit split;

    private Connection connection;
    private Statement statement;
    private ResultSet rs;

    private RecordBatchBuilder builder;

    private long count;
    private long lastSplitKey;

    public JdbcSourceReader(JdbcSource s, JdbcSource.TableSplit split) {
        this.s = s;
        this.split = split;
    }

    @Override
    public void open() throws DataSourceException {
        open(null);
    }

    @Override
    public void open(SourceCheckpoint checkpoint) throws DataSourceException {
        final Checkpoint cp = (Checkpoint) checkpoint;
        String query = buildSelectQuery(cp);

        logger.debug("Query for '{}' range {}: {}", split.table, split.splitRange, query);

        try {
            connection = DriverManager.getConnection(s.conf.getUrl(), s.connProps);
            connection.setReadOnly(true);
            statement = connection.createStatement();
            rs = statement.executeQuery(query);

            // Skip `cp.nextRowNum` rows for non-splittable table with checkpoint
            if (!split.isSplittable() && cp != null && cp.nextRowNum != null) {
                skipRecords(rs, cp.nextRowNum);
            }
        } catch (SQLException ex) {
            throw new DataSourceException(ex);
        }

        this.builder = new RecordBatchBuilder(s.globals.getBatchSize());
        this.count = 0;
    }

    @Override
    public RecordBatch readBatch() throws DataSourceException {
        while (builder.size() < s.globals.getBatchSize()) {
            Record record;
            try {
                record = readRecord();
            } catch (SQLException ex) {
                throw new DataSourceException("read next record failed", ex);
            }
            if (record == null) {
                break;
            }
            builder.addRow(record);
        }
        if (builder.size() > 0) {
            count += builder.size();
            return builder.buildAndReset();
        } else {
            return null;
        }
    }

    private Record readRecord() throws SQLException {
        if (!rs.next()) {
            return null;
        }

        final RecordType type = split.table.getType();
        Object[] values = new Object[type.getFieldCount()];
        for (int i = 0; i < type.getFields().size(); i++) {
            switch (type.getField(i).getType()) {
            case BOOLEAN:
                values[i] = rs.getBoolean(i + 1);
                break;
            case INT:
                values[i] = rs.getInt(i + 1);
                break;
            case LONG:
                values[i] = rs.getLong(i + 1);
                break;
            case FLOAT:
                values[i] = rs.getFloat(i + 1);
                break;
            case DOUBLE:
                values[i] = rs.getDouble(i + 1);
                break;
            case STRING:
                values[i] = rs.getString(i + 1);
                break;
            case BINARY:
                values[i] = new ByteArray(rs.getBytes(i + 1));
                break;
            }
            if (rs.wasNull()) {
                values[i] = null;
            }
        }

        if (split.isSplittable()) {
            lastSplitKey = rs.getLong(split.splitKey);
            assert !rs.wasNull();
        }

        return new Record(type, values);
    }

    @Override
    public void close() throws DataSourceException {
        try {
            if (rs != null) {
                rs.close();
            }
            if (statement != null) {
                statement.close();
            }
            if (connection != null) {
                connection.close();
            }
        } catch (SQLException ex) {
            throw new DataSourceException(ex);
        }
    }

    @Override
    public SourceCheckpoint checkpoint() {
        Checkpoint cp = new Checkpoint();
        if (count == 0) {
            return cp;
        } else if (split.isSplittable()) {
            cp.setLastPrimaryKey(lastSplitKey);
        } else {
            cp.setNextRowNum(count);
        }
        return cp;
    }

    private String buildSelectQuery(Checkpoint cp) {
        List<String> selectFields = split.table.getType().getFields().stream()
            .map(Field::getName)
            .map(this::quote)
            .collect(Collectors.toList());
        if (!split.isSplittable()) {
            return "SELECT " + String.join(", ", selectFields)
                + " FROM " + quote(split.table.getName());
        }

        // splitKey is used for checkpoint so it must be included in the select fields
        if (!selectFields.contains(split.splitKey)) {
            // append to the last one field
            selectFields = new ImmutableList.Builder<String>()
                .addAll(selectFields)
                .add(quote(split.splitKey))
                .build();
        }

        // Build WHERE conditions for split range and checkpoint
        List<String> conditions = buildRangeConditions();
        if (cp != null && cp.lastPrimaryKey != null) {
            conditions.add(split.splitKey + " > " + cp.lastPrimaryKey);
        }

        // Build SQL statement
        StringBuilder query = new StringBuilder();
        query.append("SELECT ").append(String.join(", ", selectFields))
            .append(" FROM ").append(quote(split.table.getName()));
        if (!conditions.isEmpty()) {
            query.append(" WHERE ").append(String.join(" AND ", conditions));
        }
        return query.toString();
    }

    private List<String> buildRangeConditions() {
        List<String> conditions = new ArrayList<>();
        if (split.splitRange.hasLowerBound()) {
            if (split.splitRange.lowerBoundType() == BoundType.CLOSED) {
                conditions.add(quote(split.splitKey) + " >= " + split.splitRange.lowerEndpoint());
            } else {
                conditions.add(quote(split.splitKey) + " > " + split.splitRange.lowerEndpoint());
            }
        }
        if (split.splitRange.hasUpperBound()) {
            if (split.splitRange.upperBoundType() == BoundType.CLOSED) {
                conditions.add(quote(split.splitKey) + " <= " + split.splitRange.upperEndpoint());
            } else {
                conditions.add(quote(split.splitKey) + " < " + split.splitRange.upperEndpoint());
            }
        }
        return conditions;
    }

    private String quote(String identifier) {
        return s.quoteIdentifier(identifier);
    }

    private void skipRecords(ResultSet rs, long skip) throws SQLException {
        for (long i = 0; i < skip; i++) {
            if (!rs.next()) {
                break;
            }
        }
    }

    @Data
    public static class Checkpoint implements SourceCheckpoint {
        /**
         * Saves last primary key when the table is split by primary key
         */
        private Long lastPrimaryKey;

        /**
         * Saves next row number (starting from 0) when table is not splittable
         */
        private Long nextRowNum;
    }
}
