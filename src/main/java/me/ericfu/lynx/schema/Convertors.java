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
        register(BOOLEAN, INT, v -> ((Boolean) v) ? 1 : 0);
        register(BOOLEAN, LONG, v -> ((Boolean) v) ? 1L : 0L);
        register(INT, LONG, v -> (long) ((Integer) v));
        register(FLOAT, DOUBLE, v -> (double) ((Float) v));

        register(LONG, STRING, v -> Long.toString((Long) v));
        register(FLOAT, STRING, v -> Float.toString((Float) v));
        register(DOUBLE, STRING, v -> Double.toString((Double) v));

        register(STRING, LONG, v -> Long.parseLong((String) v));
        register(STRING, FLOAT, v -> Float.parseFloat((String) v));
        register(STRING, DOUBLE, v -> Double.parseDouble((String) v));

        register(BOOLEAN, BOOLEAN, IDENTICAL);
        register(INT, INT, IDENTICAL);
        register(LONG, LONG, IDENTICAL);
        register(FLOAT, FLOAT, IDENTICAL);
        register(DOUBLE, DOUBLE, IDENTICAL);
        register(STRING, STRING, IDENTICAL);
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
        Convertor nullSafeConvertor = v -> v == null ? null : convertor.convert(v);
        Convertor old = convertors.put(new Pair<>(from, to), nullSafeConvertor);
        if (old != null) {
            throw new AssertionError("convertor (" + from + " -> " + to + ") is duplicated");
        }
    }
}
