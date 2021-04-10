package me.ericfu.lynx.source.random;

import com.google.common.collect.ImmutableMap;
import me.ericfu.lynx.data.ByteArray;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.source.SourceFactory;
import me.ericfu.lynx.source.SourceTest;
import me.ericfu.lynx.source.random.RandomSourceConf.ColumnSpec;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static me.ericfu.lynx.schema.type.BasicType.*;
import static org.junit.Assert.*;

public class RandomSourceTest extends SourceTest {

    private RandomSource buildSourceForTest() {

        ColumnSpec t1c1 = new ColumnSpec();
        t1c1.setName("c1");
        t1c1.setType(LONG);
        t1c1.setRule("rownum");

        ColumnSpec t1c2 = new ColumnSpec();
        t1c2.setName("c2");
        t1c2.setType(STRING);
        t1c2.setRule("{ return String.format(\"%06d\", rownum); }");

        BasicType[] types = new BasicType[]{INT, LONG, FLOAT, DOUBLE, BOOLEAN, STRING, BINARY};
        ColumnSpec[] t2c = new ColumnSpec[types.length];

        for (int i = 0; i < types.length; i++) {
            t2c[i] = new ColumnSpec();
            t2c[i].setName("c" + i);
            t2c[i].setType(types[i]);
        }

        RandomSourceConf conf = new RandomSourceConf();
        conf.setRecords(10);
        conf.setTables(ImmutableMap.of(
            "t1", Arrays.asList(t1c1, t1c2),
            "t2", Arrays.asList(t2c)
        ));

        return (RandomSource) new SourceFactory().create(globals, conf);
    }

    @Test
    public void testSimple() throws Exception {
        RandomSource source = buildSourceForTest();
        source.init();

        Schema schema = source.getSchema();
        assertEquals(2, schema.getTables().size());
        RandomSourceTable t1 = (RandomSourceTable) schema.getTable("t1");
        assertEquals(2, t1.getType().getFieldCount());

        assertEquals(Arrays.asList(LONG, STRING), t1.getType().getFields()
            .stream().map(Field::getType)
            .collect(Collectors.toList()));

        { // Check t1
            List<SourceTest.ReadResult> results = readAllRecords(source, t1);
            assertEquals(Arrays.asList(0, 0, 0, 0, 0, 1, 1, 1, 1, 1),
                results.stream().map(r -> r.reader).collect(Collectors.toList()));
            assertEquals(Arrays.asList(0, 0, 0, 1, 1),
                results.stream().filter(r -> r.reader == 0).map(r -> r.batch).collect(Collectors.toList()));
            results.stream().map(r -> r.record).forEach(record -> {
                assertNotNull(record.getValue(0));
                assertTrue(record.getValue(0) instanceof Long);
                assertNotNull(record.getValue(1));
                assertTrue(record.getValue(1) instanceof String);
            });
        }

        { // Check t2
            List<ReadResult> results = readAllRecords(source, schema.getTable("t2"));
            results.stream().map(r -> r.record).forEach(record -> {
                assertNotNull(record.getValue(4));
                assertTrue(record.getValue(4) instanceof Boolean);
                assertNotNull(record.getValue(6));
                assertTrue(record.getValue(6) instanceof ByteArray);
            });
        }
    }

    @Test
    public void testCheckpoint() throws Exception {
        RandomSource source = buildSourceForTest();
        source.init();
        Schema schema = source.getSchema();

        { // Check t1
            RandomSourceReader.Checkpoint cp0 = new RandomSourceReader.Checkpoint();
            cp0.setNextRowNum(1L); // 0 <here> 1 2 3 4
            RandomSourceReader.Checkpoint cp1 = new RandomSourceReader.Checkpoint();
            cp1.setNextRowNum(9L); // 5 6 7 8 <here> 9

            List<SourceTest.ReadResult> results = readAllRecords(source, schema.getTable("t1"), Arrays.asList(cp0, cp1));
            assertEquals(Arrays.asList(0, 0, 0, 0, 1),
                results.stream().map(r -> r.reader).collect(Collectors.toList()));
            assertEquals(Arrays.asList(0, 0, 0, 1),
                results.stream().filter(r -> r.reader == 0).map(r -> r.batch).collect(Collectors.toList()));
        }

        { // Check t2
            RandomSourceReader.Checkpoint cp0 = new RandomSourceReader.Checkpoint();
            cp0.setNextRowNum(0L); // <here> 0 1 2 3 4
            RandomSourceReader.Checkpoint cp1 = new RandomSourceReader.Checkpoint();
            cp1.setNextRowNum(10L); // 5 6 7 8 9 <here>

            List<ReadResult> results = readAllRecords(source, schema.getTable("t2"), Arrays.asList(cp0, cp1));
            assertEquals(Arrays.asList(0, 0, 0, 0, 0),
                results.stream().map(r -> r.reader).collect(Collectors.toList()));
            assertEquals(Arrays.asList(0, 0, 0, 1, 1),
                results.stream().filter(r -> r.reader == 0).map(r -> r.batch).collect(Collectors.toList()));
        }
    }
}
