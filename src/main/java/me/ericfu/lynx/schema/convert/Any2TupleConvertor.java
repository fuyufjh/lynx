package me.ericfu.lynx.schema.convert;

import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBuilder;
import me.ericfu.lynx.schema.type.RecordType;
import me.ericfu.lynx.schema.type.TupleType;

import java.util.List;
import java.util.function.Function;

/**
 * Convert any Record to a Tuple, including Struct and Tuple Type
 */
class Any2TupleConvertor implements RecordConvertor {

    private final RecordType from;
    private final TupleType to;
    private final List<Function<Record, Object>> projections;

    public Any2TupleConvertor(RecordType from, TupleType to, List<Function<Record, Object>> projections) {
        this.from = from;
        this.to = to;
        this.projections = projections;
    }

    @Override
    public Record convert(Record in) {
        RecordBuilder builder = new RecordBuilder(to);
        for (int i = 0; i < to.getFieldCount(); i++) {
            builder.set(i, projections.get(i).apply(in));
        }
        return builder.build();
    }
}
