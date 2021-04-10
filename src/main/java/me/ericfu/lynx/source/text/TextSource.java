package me.ericfu.lynx.source.text;

import com.google.common.collect.ImmutableList;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Field;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.SchemaBuilder;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.source.SchemalessSource;
import me.ericfu.lynx.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.charset.Charset;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TextSource implements SchemalessSource {

    private static final Logger logger = LoggerFactory.getLogger(TextSource.class);

    final GeneralConf globals;
    final TextSourceConf conf;

    byte sep;
    Schema schema;
    List<File> files;
    Charset charset;

    public TextSource(GeneralConf globals, TextSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    public void provideSchema(Schema schema) {
        SchemaBuilder sb = new SchemaBuilder();
        for (Table t : schema.getTables()) {
            StructType.Builder builder = new StructType.Builder();
            for (Field field : t.getType().getFields()) {
                // Always provide strings regardless of the requested type
                builder.addField(field.getName(), BasicType.STRING);
            }
            StructType type = builder.build();
            sb.addTable(new Table(t.getName(), type));
        }
        this.schema = sb.build();
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public void init() throws DataSourceException {
        // Check file existence
        final File file = new File(conf.getPath());
        if (!file.exists()) {
            throw new DataSourceException("file or folder '" + conf.getPath() + "' not exist");
        }

        if (file.isDirectory()) {
            // Scan all files under this dir (exclude dot files)
            File[] files = file.listFiles((dir, name) -> !name.startsWith("."));
            assert files != null;
            if (!Arrays.stream(files).allMatch(File::isFile)) {
                throw new DataSourceException("invalid directory structure");
            }
            this.files = ImmutableList.copyOf(files);
        } else {
            // Single text file
            this.files = ImmutableList.of(file);
        }

        if (this.files.size() < globals.getThreads()) {
            logger.warn("Number of input files ({}) is less than number of threads ({})",
                this.files.size(), globals.getThreads());
        }

        charset = Charset.forName(conf.getEncoding());
    }

    @Override
    public List<SourceReader> createReaders(Table table) {
        return files.stream()
            .map(f -> new TextSourceReader(this, table.getType(), f))
            .collect(Collectors.toList());
    }
}
