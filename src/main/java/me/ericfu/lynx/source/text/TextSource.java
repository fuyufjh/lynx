package me.ericfu.lynx.source.text;

import com.google.common.collect.ImmutableMap;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.conf.GeneralConf;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.schema.type.TupleType;
import me.ericfu.lynx.source.Source;
import me.ericfu.lynx.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.util.*;
import java.util.stream.Collectors;

public class TextSource implements Source {

    private static final Logger logger = LoggerFactory.getLogger(TextSource.class);

    final GeneralConf globals;
    final TextSourceConf conf;

    byte sep;
    Schema schema;
    Charset charset;

    public TextSource(GeneralConf globals, TextSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    @Override
    public void init() throws DataSourceException {
        // Check file existence
        final File file = new File(conf.getPath());
        if (!file.exists()) {
            throw new DataSourceException("file or folder '" + conf.getPath() + "' not exist");
        }

        try {
            Map<String, Collection<File>> tableFiles = findAllFiles(file);
            schema = buildSchemaFromFiles(tableFiles);
        } catch (IOException e) {
            throw new DataSourceException(e);
        }

        charset = Charset.forName(conf.getEncoding());
    }

    @Override
    public Schema getSchema() {
        return schema;
    }

    @Override
    public List<SourceReader> createReaders(Table table) {
        assert table instanceof TextSourceTable;
        return ((TextSourceTable) table).getFiles().stream()
            .map(f -> new TextSourceReader(this, ((TextSourceTable) table).getType(), f))
            .collect(Collectors.toList());
    }

    /**
     * Find all files under specified folder
     *
     * @param root the file or root folder
     * @return table (named according to the directory or file name) and the files associated with it
     */
    private Map<String, Collection<File>> findAllFiles(File root) throws DataSourceException, IOException {
        if (root.isDirectory()) {
            // Scan all files under this dir (exclude dot files)
            File[] files = root.listFiles((dir, name) -> !name.startsWith("."));
            if (files == null) {
                throw new IOException("cannot list files under " + root.getPath());
            }
            if (Arrays.stream(files).allMatch(File::isFile)) {
                return ImmutableMap.of(root.getName(), Arrays.asList(files));
            } else if (Arrays.stream(files).allMatch(File::isDirectory)) {
                // Find in sub-directory recursively
                ImmutableMap.Builder<String, Collection<File>> builder = ImmutableMap.builder();
                for (File dir : files) {
                    Map<String, Collection<File>> fileInDir = findAllFiles(dir);
                    for (Map.Entry<String, Collection<File>> e : fileInDir.entrySet()) {
                        // Inner files are named with 'path/to/the/file'
                        builder.put(root.getName() + '/' + e.getKey(), e.getValue());
                    }
                }
                return builder.build();
            } else {
                throw new DataSourceException("Invalid directory structure");
            }
        } else {
            // Single text file
            final String fileName = root.getName();
            String table = fileName.substring(0, fileName.indexOf('.'));
            return ImmutableMap.of(table, Collections.singleton(root));
        }
    }

    private Schema buildSchemaFromFiles(Map<String, Collection<File>> tableFiles) throws IOException {
        Schema.Builder builder = new Schema.Builder();
        for (Map.Entry<String, Collection<File>> e : tableFiles.entrySet()) {
            final String table = e.getKey();
            final Collection<File> files = e.getValue();
            assert !files.isEmpty();
            File file = files.stream().findFirst().get();
            builder.addTable(new TextSourceTable(table, buildTypeFromFile(file), files));
        }
        return builder.build();
    }

    private TupleType buildTypeFromFile(File file) throws IOException {
        try (InputStream in = new BufferedInputStream(new FileInputStream(file))) {
            TextValueReader reader = new TextValueReader(in, charset, sep);
            TupleType.Builder type = new TupleType.Builder();
            do {
                reader.readString();
                type.addField(BasicType.STRING);
            } while (reader.isEndWithSeparator());
            assert reader.isEndWithNewLine();
            return type.build();
        }
    }

}
