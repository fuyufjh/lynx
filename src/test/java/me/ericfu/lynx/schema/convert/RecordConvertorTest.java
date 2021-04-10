package me.ericfu.lynx.schema.convert;

import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.schema.type.TupleType;
import org.junit.Assert;
import org.junit.Test;

public class RecordConvertorTest {

    @Test
    public void testStructToStruct() {
        StructType fromType = new StructType.Builder()
            .addField("c1", BasicType.INT)
            .addField("c2", BasicType.DOUBLE)
            .build();

        StructType toType = new StructType.Builder()
            .addField("c1", BasicType.LONG)
            .addField("c3", BasicType.INT)
            .addField("c2", BasicType.STRING)
            .build();

        RecordConvertor convertor = RecordConvertors.getConvertor(fromType, toType);

        Assert.assertEquals(new Record(12L, null, "3.4"), convertor.convert(new Record(12, 3.4)));
        Assert.assertEquals(new Record(null, null, null), convertor.convert(new Record(null, null)));
    }

    @Test
    public void testTupleToStruct() {
        TupleType fromType = new TupleType.Builder()
            .addField(BasicType.INT)
            .addField(BasicType.DOUBLE)
            .build();

        StructType toType = new StructType.Builder()
            .addField("c1", BasicType.LONG)
            .addField("c2", BasicType.STRING)
            .build();

        RecordConvertor convertor = RecordConvertors.getConvertor(fromType, toType);

        Assert.assertEquals(new Record(12L, "3.4"), convertor.convert(new Record(12, 3.4)));
        Assert.assertEquals(new Record(null, null), convertor.convert(new Record(null, null)));
    }

    @Test
    public void testStructToTuple() {
        StructType fromType = new StructType.Builder()
            .addField("c1", BasicType.INT)
            .addField("c2", BasicType.DOUBLE)
            .build();

        TupleType toType = new TupleType.Builder()
            .addField(BasicType.LONG)
            .addField(BasicType.STRING)
            .build();

        RecordConvertor convertor = RecordConvertors.getConvertor(fromType, toType);

        Assert.assertEquals(new Record(12L, "3.4"), convertor.convert(new Record(12, 3.4)));
        Assert.assertEquals(new Record(null, null), convertor.convert(new Record(null, null)));
    }

    @Test
    public void testTupleToTuple() {
        TupleType fromType = new TupleType.Builder()
            .addField(BasicType.INT)
            .addField(BasicType.DOUBLE)
            .build();

        TupleType toType = new TupleType.Builder()
            .addField(BasicType.LONG)
            .addField(BasicType.STRING)
            .build();

        RecordConvertor convertor = RecordConvertors.getConvertor(fromType, toType);

        Assert.assertEquals(new Record(12L, "3.4"), convertor.convert(new Record(12, 3.4)));
        Assert.assertEquals(new Record(null, null), convertor.convert(new Record(null, null)));
    }
}
