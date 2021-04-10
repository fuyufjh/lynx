package me.ericfu.lynx.schema.convert;

import com.google.common.base.Preconditions;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBuilder;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.type.TupleType;

import java.util.List;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Convert tuple record to another tuple record.
 * <p>
 * The input and output tuple must be of same size, then it will convert each field with field convertor
 */
class TupleTupleRecordConvertor implements RecordConvertor {

    private final TupleType from;
    private final TupleType to;

    private final boolean identical;

    /**
     * Project each field in output tuple
     */
    private final List<Function<Record, Object>> projections;

    public TupleTupleRecordConvertor(TupleType from, TupleType to) {
        Preconditions.checkArgument(from.getFieldCount() == to.getFieldCount());
        this.from = from;
        this.to = to;
        this.identical = from.getFieldCount() == to.getFieldCount()
            && IntStream.range(0, from.getFieldCount())
            .allMatch(i -> from.getField(i).getType() == to.getField(i).getType());
        this.projections = to.getFields().stream()
            .map(toField -> {
                Field fromField = from.getField(toField.getOrdinal());
                assert fromField != null; // already checked in `checkCompatible`
                final Convertor convertor = Convertors.getConvertor(fromField.getType(), toField.getType());
                assert convertor != null; // same as above
                final int ordinal = fromField.getOrdinal();
                return (Function<Record, Object>) record -> convertor.convert(record.getValue(ordinal));
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
