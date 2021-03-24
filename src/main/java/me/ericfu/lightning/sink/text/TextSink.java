package me.ericfu.lightning.sink.text;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.Schema;
import me.ericfu.lightning.schema.Table;
import me.ericfu.lightning.sink.SchemalessSink;
import me.ericfu.lightning.sink.SinkWriter;

import java.io.File;

public class TextSink implements SchemalessSink {

    final GeneralConf globals;
    final TextSinkConf conf;

    Schema schema;

    public TextSink(GeneralConf globals, TextSinkConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSinkException {
        File dir = new File(conf.getPath());
        if (!dir.exists()) {
            throw new DataSinkException("path not exists");
        }
        if (!dir.isDirectory()) {
            throw new DataSinkException("path is not directory");
        }
    }

    @Override
    public void provideSchema(Schema schema) throws IncompatibleClassChangeError {
        this.schema = schema;
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public SinkWriter createWriter(Table table) {
        return null; // TODO
    }
}
