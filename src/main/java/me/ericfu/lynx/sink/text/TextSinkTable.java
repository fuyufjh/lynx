package me.ericfu.lynx.sink.text;

import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.TupleType;

class TextSinkTable extends Table {

    public TextSinkTable(String name, TupleType type) {
        super(name, type);
    }

    @Override
    public TupleType getType() {
        return (TupleType) super.getType();
    }
}
