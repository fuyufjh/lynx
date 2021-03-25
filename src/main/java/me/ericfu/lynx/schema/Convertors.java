package me.ericfu.lynx.schema;

import javafx.util.Pair;

import javax.annotation.Nullable;
import java.util.HashMap;
import java.util.Map;

import static me.ericfu.lynx.schema.BasicType.*;

public abstract class Convertors {

    private static final Convertor IDENTICAL = v -> v;

    private static final Map<Pair<BasicType, BasicType>, Convertor> convertors = new HashMap<>();

    static {
        register(INT64, STRING, v -> v == null ? null : Long.toString((Long) v));
        register(FLOAT, STRING, v -> v == null ? null : Float.toString((Float) v));
        register(DOUBLE, STRING, v -> v == null ? null : Double.toString((Double) v));

        register(STRING, INT64, v -> v == null ? null : Long.parseLong((String) v));
        register(STRING, FLOAT, v -> v == null ? null : Float.parseFloat((String) v));
        register(STRING, DOUBLE, v -> v == null ? null : Double.parseDouble((String) v));

        register(STRING, STRING, IDENTICAL);
        register(INT64, INT64, IDENTICAL);
        register(FLOAT, FLOAT, IDENTICAL);
        register(DOUBLE, DOUBLE, IDENTICAL);
        register(BINARY, BINARY, IDENTICAL);
    }

    /**
     * Get a Convertor to convert from source type to target type
     *
     * @param from source type
     * @param to   target type
     * @return a convertor as required, or null if this kind of conversion not supported
     */
    @Nullable
    public static Convertor getConvertor(BasicType from, BasicType to) {
        return convertors.get(new Pair<>(from, to));
    }

    private static void register(BasicType from, BasicType to, Convertor convertor) {
        Convertor old = convertors.put(new Pair<>(from, to), convertor);
        if (old != null) {
            throw new AssertionError("convertor (" + from + " -> " + to + ") is duplicated");
        }
    }
}
