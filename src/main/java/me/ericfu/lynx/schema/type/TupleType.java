package me.ericfu.lynx.schema.type;

import com.google.common.collect.ImmutableList;
import me.ericfu.lynx.schema.Field;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

/**
 * TupleType contains an ordered list of nameless fields (columns)
 */
public class TupleType implements RecordType {

    /**
     * Fields in nature order
     */
    private final List<Field> fields;

    public TupleType(List<Field> fields) {
        this.fields = ImmutableList.copyOf(fields);
    }

    public int getFieldCount() {
        return fields.size();
    }

    public Field getField(int index) {
        return fields.get(index);
    }

    public List<Field> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return fields.stream()
            .map(f -> f.getType().name())
            .collect(Collectors.joining(", ", "[", "]"));
    }

    public static class Builder {

        private final List<Field> fields;

        public Builder() {
            this.fields = new ArrayList<>();
        }

        public Builder addField(BasicType type) {
            final int ordinal = fields.size();
            fields.add(new Field(ordinal, "$" + ordinal, type));
            return this;
        }

        public TupleType build() {
            return new TupleType(fields);
        }
    }
}
