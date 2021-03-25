package me.ericfu.lynx.sink.text;

import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.sink.SchemalessSink;
import me.ericfu.lynx.sink.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;

public class TextSink implements SchemalessSink {

    private static final Logger logger = LoggerFactory.getLogger(TextSink.class);

    final GeneralConf globals;
    final TextSinkConf conf;

    Schema schema;
    Charset charset;

    /**
     * Count of writers for each table
     */
    Map<String, Integer> writerCount = new HashMap<>();

    public TextSink(GeneralConf globals, TextSinkConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSinkException {
        File dir = new File(conf.getPath());
        if (!dir.exists()) {
            logger.info("Path '" + conf.getPath() + "' not exist and will be created");
            if (!dir.mkdirs()) {
                throw new DataSinkException("cannot create directory " + dir.getPath());
            }
        }
        if (!dir.isDirectory()) {
            throw new DataSinkException(dir.getPath() + " is not directory");
        }

        charset = Charset.forName(conf.getEncoding());
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
        int partition = writerCount.compute(table.getName(), (name, count) -> count == null ? 0 : count + 1);
        String fileName = String.format("%s/%d.txt", table.getName(), partition);
        Path targetPath = Paths.get(conf.getPath(), fileName);
        return new TextSinkWriter(this, targetPath.toFile(), table, charset);
    }
}
