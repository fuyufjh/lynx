package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.schema.BasicType;

import java.sql.Types;
import java.util.Map;
import java.util.Properties;

/**
 * Utilities for JDBC Source and Sink
 */
public abstract class JdbcUtils {

    public static BasicType convertJdbcType(int jdbcType) {
        switch (jdbcType) {
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
        case Types.BIT:
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
}
