package me.ericfu.lightning.source.text;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.BasicType;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.schema.RecordTypeBuilder;
import me.ericfu.lightning.source.SchemalessSource;
import me.ericfu.lightning.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

public class TextSource implements SchemalessSource {

    private static final Logger logger = LoggerFactory.getLogger(TextSource.class);

    final GeneralConf globals;
    final TextSourceConf conf;

    byte sep;
    RecordType schema;
    List<File> files;

    public TextSource(GeneralConf globals, TextSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    public void provideSchema(RecordType schema) {
        RecordTypeBuilder builder = new RecordTypeBuilder();
        for (Field field : schema.getFields()) {
            // Always provide strings regardless of the requested type
            builder.addField(field.getName(), BasicType.STRING);
        }
        this.schema = builder.build();
    }

    @Override
    public RecordType getSchema() {
        Preconditions.checkState(schema != null);
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
    }

    @Override
    public List<SourceReader> createReaders() {
        return files.stream()
            .map(f -> new TextSourceReader(this, f))
            .collect(Collectors.toList());
    }
}
