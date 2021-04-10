package me.ericfu.lynx.schema.convert;

import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.exception.IncompatibleSchemaException;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.type.RecordType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.schema.type.TupleType;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Function;
import java.util.stream.IntStream;

public abstract class RecordConvertors {

    private static final RecordConvertor IDENTICAL = r -> r;

    public static RecordConvertor create(RecordType from, RecordType to)
        throws IncompatibleSchemaException {
        if (from instanceof StructType && to instanceof StructType) {
            return createStruct2Struct((StructType) from, (StructType) to);
        } else if (from instanceof TupleType && to instanceof TupleType) {
            return createTuple2Tuple((TupleType) from, (TupleType) to);
        } else {
            throw new AssertionError("not implemented");
        }
    }

    private static RecordConvertor createStruct2Struct(StructType from, StructType to)
        throws IncompatibleSchemaException {
        if (from.getFieldCount() == to.getFieldCount()
            && IntStream.range(0, from.getFieldCount()).allMatch(i ->
            from.getField(i).getName().equals(to.getField(i).getName())
                && from.getField(i).getType() == to.getField(i).getType())) {
            return IDENTICAL;
        } else {
            List<Function<Record, Object>> projections = new ArrayList<>();
            for (Field toField : to.getFields()) {
                // Mapping field by name
                Field fromField = from.getField(toField.getName());
                if (fromField != null) {
                    Convertor convertor = Convertors.getConvertor(fromField.getType(), toField.getType());
                    int ordinal = fromField.getOrdinal();
                    projections.add(record -> convertor.convert(record.getValue(ordinal)));
                } else {
                    projections.add(record -> null);
                }
            }
            return new Any2TupleConvertor(from, to, projections);
        }
    }

    private static RecordConvertor createTuple2Tuple(TupleType from, TupleType to)
        throws IncompatibleSchemaException {
        if (from.getFieldCount() != to.getFieldCount()) {
            throw new IncompatibleSchemaException("number of fields not matched");
        }
        if (IntStream.range(0, from.getFieldCount())
            .allMatch(i -> from.getField(i).getType() == to.getField(i).getType())) {
            return IDENTICAL;
        } else {
            List<Function<Record, Object>> projections = new ArrayList<>();
            for (Field toField : to.getFields()) {
                // Mapping field by ordinal
                Field fromField = from.getField(toField.getOrdinal());
                Convertor convertor = Convertors.getConvertor(fromField.getType(), toField.getType());
                final int ordinal = fromField.getOrdinal();
                projections.add(record -> convertor.convert(record.getValue(ordinal)));
            }
            return new Any2TupleConvertor(from, to, projections);
        }
    }
}
