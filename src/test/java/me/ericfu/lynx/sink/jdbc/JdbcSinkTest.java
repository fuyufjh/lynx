package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.sink.SinkFactory;
import me.ericfu.lynx.sink.SinkTest;
import me.ericfu.lynx.source.jdbc.JdbcSourceConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

import static org.junit.Assert.*;

public class JdbcSinkTest extends SinkTest {

    private static final String JDBC_URL = "jdbc:hsqldb:mem:jdbc_sink_test";

    @Before
    public void setUp() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE \"t1\" (\n" +
                    "    \"id\" INTEGER IDENTITY,\n" +
                    "    \"boolean_col\" BOOLEAN,\n" +
                    "    \"int_col\" INTEGER,\n" +
                    "    \"long_col\" BIGINT,\n" +
                    "    \"float_col\" FLOAT,\n" +
                    "    \"double_col\" FLOAT,\n" +
                    "    \"string_col\" VARCHAR(100),\n" +
                    "    \"binary_col\" VARBINARY(100),\n" +
                    "    PRIMARY KEY (\"id\")\n" +
                    ")");
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE \"t1\" IF EXISTS");
            }
        }
    }

    @Test
    public void testSimple() throws Exception {
        JdbcSinkConf conf = new JdbcSinkConf();
        conf.setUrl(JDBC_URL);
        conf.setQuoteIdentifier(JdbcSourceConf.IdentifierQuotation.DOUBLE);

        JdbcSink sink = (JdbcSink) new SinkFactory().create(globals, conf);
        sink.init(sourceSchema);

        Schema schema = sink.getSchema();
        Table t1 = schema.getTable("t1");
        assertNotNull(t1);
        assertTrue(t1.getType() instanceof StructType);

        writeAllRecords(sink, t1);

        // Check written data in database
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                ResultSet rs = stmt.executeQuery(
                    "SELECT \"boolean_col\", \"long_col\", \"double_col\", \"string_col\", \"binary_col\" FROM \"t1\"");
                for (int i = 0; i < NUM_RECORDS; i++) {
                    assertTrue(rs.next());
                    assertEquals(RECORD[i].booleanVal, JdbcUtils.getValue(rs, BasicType.BOOLEAN, 1));
                    assertEquals(RECORD[i].longVal, JdbcUtils.getValue(rs, BasicType.LONG, 2));
                    assertEquals(RECORD[i].doubleVal, JdbcUtils.getValue(rs, BasicType.DOUBLE, 3));
                    assertEquals(RECORD[i].stringVal, JdbcUtils.getValue(rs, BasicType.STRING, 4));
                    assertEquals(RECORD[i].binaryVal, JdbcUtils.getValue(rs, BasicType.BINARY, 5));
                }
                assertFalse(rs.next());
                rs.close();
            }
        }
    }
}