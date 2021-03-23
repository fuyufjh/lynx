package me.ericfu.lightning.schema;

import com.google.common.base.Preconditions;
import me.ericfu.lightning.data.ByteString;
import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBuilder;

import java.nio.charset.StandardCharsets;
import java.util.stream.IntStream;

public class RecordConvertor {

    private final RecordType from;
    private final RecordType to;
    private final boolean identical;

    public RecordConvertor(RecordType from, RecordType to) {
        Preconditions.checkArgument(from.getFieldCount() == to.getFieldCount());
        this.from = from;
        this.to = to;
        this.identical = IntStream.range(0, from.getFieldCount())
            .allMatch(i -> from.getField(i).getType() == to.getField(i).getType());
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
            Object outValue;
            if (from.getField(i).getType() == to.getField(i).getType()) {
                outValue = in.getValue(i);
            } else {
                // TODO: improve performance
                String s = in.getValue(i).toString();
                outValue = new ByteString(s.getBytes(StandardCharsets.UTF_8), StandardCharsets.UTF_8);
            }
            builder.set(i, outValue);
        }
        return builder.build();
    }
}
