package me.ericfu.lightning.schema;

import com.google.common.collect.ImmutableList;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * RecordType contains an ordered list of fields(columns)
 */
public class RecordType {

    private final List<Field> fields;

    /**
     * Map from field name to field for quick lookup
     */
    private final Map<String, Field> name2Fields;

    RecordType(List<Field> fields) {
        this.fields = ImmutableList.copyOf(fields);

        this.name2Fields = new HashMap<>(fields.size());
        for (Field field : fields) {
            name2Fields.put(field.getName(), field);
        }
    }

    public int getFieldCount() {
        return fields.size();
    }

    public Field getField(int index) {
        return fields.get(index);
    }

    public Field getField(String name) {
        return name2Fields.get(name);
    }

    public List<Field> getFields() {
        return fields;
    }

    @Override
    public String toString() {
        return fields.stream()
                .map(f -> f.getName() + " " + f.getType())
                .collect(Collectors.joining(", ", "[", "]"));
    }
}
