package me.ericfu.lightning.schema;

import com.google.common.base.Preconditions;
import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBuilder;

import java.util.stream.IntStream;

public class RecordConvertor {

    private final RecordType from;
    private final RecordType to;
    private final boolean identical;
    private final Convertor[] convertors;

    public RecordConvertor(RecordType from, RecordType to) {
        Preconditions.checkArgument(from.getFieldCount() == to.getFieldCount());
        this.from = from;
        this.to = to;
        this.identical = IntStream.range(0, from.getFieldCount())
            .allMatch(i -> from.getField(i).getType() == to.getField(i).getType());
        this.convertors = IntStream.range(0, from.getFieldCount())
            .mapToObj(i -> Convertors.getConvertor(from.getField(i).getType(), to.getField(i).getType()))
            .toArray(Convertor[]::new);
    }

    boolean isIdentical() {
        return identical;
    }

    public Record convert(Record in) {
        if (identical) {
            return in;
        }
        RecordBuilder builder = new RecordBuilder(to);
        for (int i = 0; i < from.getFieldCount(); i++) {
            builder.set(i, convertors[i].convert(in.getValue(i)));
        }
        return builder.build();
    }
}
