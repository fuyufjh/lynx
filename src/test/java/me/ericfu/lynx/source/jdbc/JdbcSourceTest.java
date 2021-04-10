package me.ericfu.lynx.source.jdbc;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Streams;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.sink.jdbc.JdbcUtils;
import me.ericfu.lynx.source.SourceFactory;
import me.ericfu.lynx.source.SourceTest;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static me.ericfu.lynx.schema.type.BasicType.*;
import static org.junit.Assert.assertEquals;

public class JdbcSourceTest extends SourceTest {

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
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO \"t1\" VALUES (?,?,?,?,?,?)")) {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    ps.setInt(1, i);
                    JdbcUtils.setParameter(ps, 2, BOOLEAN, RECORD[i].booleanVal);
                    JdbcUtils.setParameter(ps, 3, LONG, RECORD[i].longVal);
                    JdbcUtils.setParameter(ps, 4, DOUBLE, RECORD[i].doubleVal);
                    JdbcUtils.setParameter(ps, 5, STRING, RECORD[i].stringVal);
                    JdbcUtils.setParameter(ps, 6, BINARY, RECORD[i].binaryVal);
                    ps.addBatch();
                }
                ps.executeBatch();
            }
            try (PreparedStatement ps = conn.prepareStatement("INSERT INTO \"t2\" VALUES (?,?,?,?,?,?)")) {
                for (int i = 0; i < NUM_RECORDS; i++) {
                    JdbcUtils.setParameter(ps, 1, BOOLEAN, RECORD[i].booleanVal);
                    JdbcUtils.setParameter(ps, 2, INT, RECORD[i].intVal);
                    JdbcUtils.setParameter(ps, 3, LONG, RECORD[i].longVal);
                    JdbcUtils.setParameter(ps, 4, DOUBLE, RECORD[i].doubleVal);
                    JdbcUtils.setParameter(ps, 5, STRING, RECORD[i].stringVal);
                    JdbcUtils.setParameter(ps, 6, BINARY, RECORD[i].binaryVal);
                    ps.addBatch();
                }
                ps.executeBatch();
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

        JdbcSourceConf conf = new JdbcSourceConf();
        conf.setUrl(JDBC_URL);
        conf.setQuoteIdentifier(JdbcSourceConf.IdentifierQuotation.DOUBLE); // ANSI Standard
        JdbcSourceConf.TableDesc t1 = new JdbcSourceConf.TableDesc();
        JdbcSourceConf.TableDesc t2 = new JdbcSourceConf.TableDesc();
        conf.setTables(ImmutableMap.of("t1", t1, "t2", t2));

        JdbcSource source = (JdbcSource) new SourceFactory().create(globals, conf);
        source.init();

        Schema schema = source.getSchema();
        assertEquals(2, schema.getTables().size());
        assertEquals(6, schema.getTable("t1").getType().getFieldCount());
        assertEquals(Arrays.asList(
            INT, BOOLEAN, LONG, DOUBLE, STRING, BINARY
        ), schema.getTable("t1").getType().getFields()
            .stream().map(Field::getType)
            .collect(Collectors.toList()));

        { // Check t1
            List<ReadResult> results = readAllRecords(source, schema.getTable("t1"));
            assertEquals(Arrays.asList(0, 0, 0, 0, 0, 1, 1, 1, 1, 1),
                results.stream().map(r -> r.reader).collect(Collectors.toList()));
            assertEquals(Arrays.asList(0, 0, 0, 1, 1),
                results.stream().filter(r -> r.reader == 0).map(r -> r.batch).collect(Collectors.toList()));

            assertEquals(
                Streams.mapWithIndex(Arrays.stream(RECORD), (r, i) ->
                    new Record((int) i, r.booleanVal, r.longVal, r.doubleVal, r.stringVal, r.binaryVal))
                    .collect(Collectors.toList()),
                results.stream().map(r -> r.record).collect(Collectors.toList())
            );
        }

        { // Check t2
            List<ReadResult> results = readAllRecords(source, schema.getTable("t2"));
            assertEquals(Arrays.asList(0, 0, 0, 0, 0, 0, 0, 0, 0, 0),
                results.stream().map(r -> r.reader).collect(Collectors.toList()));
            assertEquals(Arrays.asList(0, 0, 0, 1, 1, 1, 2, 2, 2, 3),
                results.stream().filter(r -> r.reader == 0).map(r -> r.batch).collect(Collectors.toList()));

            assertEquals(
                Arrays.stream(RECORD).map(r ->
                    new Record(r.booleanVal, r.intVal, r.longVal, r.doubleVal, r.stringVal, r.binaryVal))
                    .collect(Collectors.toList()),
                results.stream().map(r -> r.record).collect(Collectors.toList())
            );
        }
    }

    @Test
    public void testCheckpoint() throws Exception {
        GeneralConf globals = new GeneralConf();
        globals.setBatchSize(3);
        globals.setThreads(2);

        JdbcSourceConf conf = new JdbcSourceConf();
        conf.setUrl(JDBC_URL);
        conf.setQuoteIdentifier(JdbcSourceConf.IdentifierQuotation.DOUBLE); // ANSI Standard
        JdbcSourceConf.TableDesc t1 = new JdbcSourceConf.TableDesc();
        JdbcSourceConf.TableDesc t2 = new JdbcSourceConf.TableDesc();
        conf.setTables(ImmutableMap.of("t1", t1, "t2", t2));

        JdbcSource source = (JdbcSource) new SourceFactory().create(globals, conf);
        source.init();
        Schema schema = source.getSchema();

        { // Check t1
            JdbcSourceReader.Checkpoint cp0 = new JdbcSourceReader.Checkpoint();
            cp0.setLastPrimaryKey(0L); // 0 <here> 1 2 3 4
            JdbcSourceReader.Checkpoint cp1 = new JdbcSourceReader.Checkpoint();
            cp1.setLastPrimaryKey(9L); // 5 6 7 8 9 <here>

            List<ReadResult> results = readAllRecords(source, schema.getTable("t1"), Arrays.asList(cp0, cp1));
            assertEquals(Arrays.asList(0, 0, 0, 0),
                results.stream().map(r -> r.reader).collect(Collectors.toList()));
            assertEquals(Arrays.asList(0, 0, 0, 1),
                results.stream().filter(r -> r.reader == 0).map(r -> r.batch).collect(Collectors.toList()));
        }

        { // Check t2
            JdbcSourceReader.Checkpoint cp = new JdbcSourceReader.Checkpoint();
            cp.setNextRowNum(6L); // expect to skip 6 rows (4 rows left)

            List<ReadResult> results = readAllRecords(source, schema.getTable("t2"), Arrays.asList(cp));
            assertEquals(Arrays.asList(0, 0, 0, 0),
                results.stream().map(r -> r.reader).collect(Collectors.toList()));
            assertEquals(Arrays.asList(0, 0, 0, 1),
                results.stream().filter(r -> r.reader == 0).map(r -> r.batch).collect(Collectors.toList()));
        }
    }
}