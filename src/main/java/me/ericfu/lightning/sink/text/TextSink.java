package me.ericfu.lightning.sink.text;

import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.Schema;
import me.ericfu.lightning.schema.Table;
import me.ericfu.lightning.sink.SchemalessSink;
import me.ericfu.lightning.sink.SinkWriter;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

public class TextSink implements SchemalessSink {

    final GeneralConf globals;
    final TextSinkConf conf;

    Schema schema;
    int writerCount = 0;

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
        String fileName = String.format("%d.txt", writerCount++);
        Path targetPath = Paths.get(conf.getPath(), fileName);
        return new TextSinkWriter(this, targetPath.toFile(), table);
    }
}
