package me.ericfu.lynx.source.text;

import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.TupleType;

import java.io.File;
import java.util.Collection;

class TextSourceTable extends Table {

    private final Collection<File> files;

    public TextSourceTable(String name, TupleType type, Collection<File> files) {
        super(name, type);
        this.files = files;
    }

    @Override
    public TupleType getType() {
        return (TupleType) super.getType();
    }

    public Collection<File> getFiles() {
        return files;
    }
}
