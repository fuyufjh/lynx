package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.schema.BasicType;

import java.sql.Types;

abstract class JdbcUtils {

    static BasicType convertJdbcType(int jdbcType) {
        switch (jdbcType) {
        case Types.TINYINT:
        case Types.SMALLINT:
        case Types.INTEGER:
        case Types.BIGINT:
            return BasicType.INT64;
        case Types.FLOAT:
        case Types.REAL:
            return BasicType.FLOAT;
        case Types.DOUBLE:
            return BasicType.DOUBLE;
        default:
            return BasicType.STRING;
        }
    }

}
