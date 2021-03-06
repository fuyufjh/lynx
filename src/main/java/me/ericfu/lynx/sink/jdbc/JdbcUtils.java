package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.data.ByteArray;
import me.ericfu.lynx.schema.type.BasicType;

import javax.annotation.Nullable;
import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities for JDBC Source and Sink
 */
public abstract class JdbcUtils {

    public static BasicType convertJdbcType(int jdbcType) {
        switch (jdbcType) {
        case Types.BIT:
        case Types.BOOLEAN:
            return BasicType.BOOLEAN;
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
            return BasicType.INT;
        case Types.BIGINT:
            return BasicType.LONG;
        case Types.FLOAT:
        case Types.REAL:
            return BasicType.FLOAT;
        case Types.DOUBLE:
            return BasicType.DOUBLE;
        case Types.CHAR:
        case Types.VARCHAR:
        case Types.LONGVARCHAR:
        case Types.CLOB:
            return BasicType.STRING;
        case Types.BINARY:
        case Types.VARBINARY:
        case Types.LONGVARBINARY:
        case Types.BLOB:
            return BasicType.BINARY;
        default:
            // Treat other unsupported types as STRING
            return BasicType.STRING;
        }
    }

    public static int convertToJdbcType(BasicType type) {
        switch (type) {
        case BOOLEAN:
            return Types.BOOLEAN;
        case INT:
            return Types.INTEGER;
        case LONG:
            return Types.BIGINT;
        case FLOAT:
            return Types.FLOAT;
        case DOUBLE:
            return Types.DOUBLE;
        case STRING:
            return Types.VARCHAR;
        case BINARY:
            return Types.BINARY;
        default:
            throw new AssertionError();
        }
    }

    /**
     * Calls a suitable setter method of PreparedStatement to set the value
     *
     * @param i     index of parameter (starting from 1)
     * @param value value or null
     */
    public static void setParameter(PreparedStatement ps, int i, BasicType type, @Nullable Object value) throws SQLException {
        if (value == null) {
            ps.setNull(i, convertToJdbcType(type));
        } else {
            switch (type) {
            case BOOLEAN:
                ps.setBoolean(i, (boolean) value);
                break;
            case INT:
                ps.setInt(i, (int) value);
                break;
            case LONG:
                ps.setLong(i, (long) value);
                break;
            case FLOAT:
                ps.setFloat(i, (float) value);
                break;
            case DOUBLE:
                ps.setDouble(i, (double) value);
                break;
            case STRING:
                ps.setString(i, (String) value);
                break;
            case BINARY:
                ps.setBytes(i, ((ByteArray) value).getBytes());
                break;
            default:
                throw new AssertionError();
            }
        }
    }

    /**
     * Read a value from ResultSet
     *
     * @param i index of parameter (starting from 1)
     */
    public static @Nullable
    Object getValue(ResultSet rs, BasicType type, int i) throws SQLException {
        Object value;
        switch (type) {
        case BOOLEAN:
            value = rs.getBoolean(i);
            break;
        case INT:
            value = rs.getInt(i);
            break;
        case LONG:
            value = rs.getLong(i);
            break;
        case FLOAT:
            value = rs.getFloat(i);
            break;
        case DOUBLE:
            value = rs.getDouble(i);
            break;
        case STRING:
            value = rs.getString(i);
            break;
        case BINARY:
            value = new ByteArray(rs.getBytes(i));
            break;
        default:
            throw new AssertionError();
        }
        if (rs.wasNull()) {
            value = null;
        }
        return value;
    }

    /**
     * Build JDBC connection properties
     */
    public static Properties buildConnProps(String user, String password, Map<String, String> other) {
        Properties connProps = new Properties();
        if (user != null) {
            connProps.put("user", user);
        }
        if (password != null) {
            connProps.put("password", password);
        }
        if (other != null) {
            connProps.putAll(other);
        }
        return connProps;
    }

    /**
     * Fetch column types from DatabaseMetaData interface
     */
    public static Map<String, BasicType> fetchColumnTypes(DatabaseMetaData meta,
                                                          String catalog, String schema, String table)
        throws SQLException {
        // Collect column types
        Map<String, BasicType> columnTypes = new LinkedHashMap<>();
        try (ResultSet rs = meta.getColumns(catalog, schema, table, null)) {
            while (rs.next()) {
                String columnName = rs.getString("COLUMN_NAME");
                int jdbcType = rs.getInt("DATA_TYPE");
                columnTypes.put(columnName, convertJdbcType(jdbcType));
            }
        }

        if (columnTypes.isEmpty()) {
            throw new SQLException("table '" + table + "' not found");
        }
        return columnTypes;
    }
}
