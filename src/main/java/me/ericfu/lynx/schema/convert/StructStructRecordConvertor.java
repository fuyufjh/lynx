package me.ericfu.lynx.schema.convert;

import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBuilder;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.type.StructType;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Convert StructRecord to StructRecord
 * <p>
 * All the input fields must be contained in the output type, then it will convert each presented field with
 * field convertor, and set those field not presented as null
 */
class StructStructRecordConvertor implements RecordConvertor {

    private final StructType from;
    private final StructType to;

    private final boolean identical;

    /**
     * Project each field in output tuple
     */
    private final List<Function<Record, Object>> projections;

    public StructStructRecordConvertor(StructType from, StructType to) {
        this.from = from;
        this.to = to;
        this.identical = from.getFieldCount() == to.getFieldCount()
            && IntStream.range(0, from.getFieldCount())
            .allMatch(i -> from.getField(i).getName().equals(to.getField(i).getName())
                && from.getField(i).getType() == to.getField(i).getType());
        this.projections = to.getFields().stream()
            .map(toField -> {
                Field fromField = from.getField(toField.getName());
                if (fromField != null) {
                    final Convertor convertor = Convertors.getConvertor(fromField.getType(), toField.getType());
                    assert convertor != null; // same as above
                    final int ordinal = fromField.getOrdinal();
                    return (Function<Record, Object>) record -> convertor.convert(record.getValue(ordinal));
                } else {
                    return (Function<Record, Object>) record -> null;
                }
            }).collect(Collectors.toList());
    }

    @Override
    public Record convert(Record in) {
        if (identical) {
            return in;
        }
        RecordBuilder builder = new RecordBuilder(to);
        for (int i = 0; i < to.getFieldCount(); i++) {
            builder.set(i, projections.get(i).apply(in));
        }
        return builder.build();
    }
}
