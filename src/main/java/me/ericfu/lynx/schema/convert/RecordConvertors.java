package me.ericfu.lynx.schema.convert;

import me.ericfu.lynx.schema.type.RecordType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.schema.type.TupleType;

public abstract class RecordConvertors {

    public static RecordConvertor getConvertor(RecordType from, RecordType to) {
        if (from instanceof StructType && to instanceof StructType) {
            return new StructStructRecordConvertor((StructType) from, (StructType) to);
        } else if (from instanceof TupleType && to instanceof TupleType) {
            return new TupleTupleRecordConvertor((TupleType) from, (TupleType) to);
        } else {
            throw new AssertionError("not implemented");
        }
    }

}
