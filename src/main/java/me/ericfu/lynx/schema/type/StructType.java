package me.ericfu.lynx.schema.type;

import me.ericfu.lynx.schema.Field;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * StructType contains an ordered list of named fields (columns)
 */
public class StructType extends TupleType implements RecordType {

    /**
     * Map from field name to field for quick lookup
     */
    private final Map<String, Field> name2Fields;

    StructType(List<Field> fields) {
        super(fields);

        this.name2Fields = new HashMap<>(fields.size());
        for (Field field : fields) {
            name2Fields.put(field.getName(), field);
        }
    }

    public Field getField(String name) {
        return name2Fields.get(name);
    }

    @Override
    public String toString() {
        return getFields().stream()
            .map(f -> f.getName() + " " + f.getType())
            .collect(Collectors.joining(", ", "[", "]"));
    }

    public static class Builder {

        private final List<Field> fields;

        public Builder() {
            this.fields = new ArrayList<>();
        }

        public Builder addField(String name, BasicType type) {
            final int ordinal = fields.size();
            fields.add(new Field(ordinal, name, type));
            return this;
        }

        public StructType build() {
            return new StructType(fields);
        }
    }
}
