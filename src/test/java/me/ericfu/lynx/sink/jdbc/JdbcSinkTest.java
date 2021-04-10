package me.ericfu.lynx.sink.jdbc;

import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.sink.SinkTest;
import me.ericfu.lynx.source.jdbc.JdbcSourceConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.Statement;

public class JdbcSinkTest extends SinkTest {

    private static final String JDBC_URL = "jdbc:hsqldb:mem:jdbc_source_test";

    @Before
    public void setUp() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("CREATE TABLE \"t1\" (\n" +
                    "    \"id\" INT NOT NULL,\n" +
                    "    \"boolean_col\" BOOLEAN,\n" +
                    "    \"long_col\" BIGINT,\n" +
                    "    \"double_col\" FLOAT,\n" +
                    "    \"string_col\" VARCHAR(100),\n" +
                    "    \"binary_col\" VARBINARY(100),\n" +
                    "    PRIMARY KEY (\"id\")\n" +
                    ")");
                stmt.executeUpdate("CREATE TABLE \"t2\" (\n" +
                    "    \"boolean_col\" BOOLEAN,\n" +
                    "    \"int_col\" INT,\n" +
                    "    \"long_col\" BIGINT,\n" +
                    "    \"double_col\" FLOAT,\n" +
                    "    \"string_col\" VARCHAR(100),\n" +
                    "    \"binary_col\" VARBINARY(100)\n" +
                    ")");
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        try (Connection conn = DriverManager.getConnection(JDBC_URL)) {
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE \"t1\" IF EXISTS");
                stmt.executeUpdate("DROP TABLE \"t2\" IF EXISTS");
            }
        }
    }

    @Test
    public void testSimple() throws Exception {
        GeneralConf globals = new GeneralConf();
        globals.setBatchSize(3);
        globals.setThreads(2);

        JdbcSinkConf conf = new JdbcSinkConf();
        conf.setUrl(JDBC_URL);
        conf.setQuoteIdentifier(JdbcSourceConf.IdentifierQuotation.DOUBLE);


    }
}