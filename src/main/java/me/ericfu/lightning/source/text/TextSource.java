package me.ericfu.lightning.source.text;

import com.google.common.base.Preconditions;
import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.data.Batch;
import me.ericfu.lightning.data.BatchBuilder;
import me.ericfu.lightning.data.ByteString;
import me.ericfu.lightning.data.Row;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.source.SchemalessSource;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class TextSource implements SchemalessSource {

    private final GeneralConf globals;
    private final TextSourceConf conf;

    private byte sep;
    private RecordType schema;
    private BufferedInputStream in;

    private TextValueReader valueReader;
    private BatchBuilder builder;

    public TextSource(GeneralConf globals, TextSourceConf conf) {
        this.globals = globals;
        this.conf = conf;
    }

    public void setSchema(RecordType schema) {
        this.schema = Preconditions.checkNotNull(schema);
    }

    public void open() throws DataSourceException {
        if (conf.getSeparator().length() == 1 && conf.getSeparator().charAt(0) < 0x80) {
            sep = (byte) conf.getSeparator().charAt(0);
        } else {
            throw new DataSourceException("bad separator '" + conf.getSeparator() + "'");
        }

        final File file = new File(conf.getPath());
        if (!file.exists()) {
            throw new DataSourceException("file or directory '" + conf.getPath() + "' not exist");
        }
        if (file.isDirectory()) {
            throw new DataSourceException("not implemented");
        } else {
            try {
                openSingleFile(file);
            } catch (IOException ex) {
                throw new DataSourceException(ex);
            }
        }

        valueReader = new TextValueReader(in, sep);
        builder = new BatchBuilder(globals.getBatchSize());
    }

    private void openSingleFile(File file) throws IOException {
        this.in = new BufferedInputStream(new FileInputStream(file));
    }

    public Batch readBatch() throws DataSourceException {
        while (builder.size() < globals.getBatchSize()) {
            Row row;
            try {
                row = readNextRow();
            } catch (IOException ex) {
                throw new DataSourceException(ex);
            }
            if (row == null) {
                break;
            }
            builder.addRow(row);
        }
        if (builder.size() > 0) {
            return builder.buildAndReset();
        } else {
            return null;
        }
    }

    private Row readNextRow() throws IOException {
        Object[] values = new Object[schema.getFieldCount()];
        for (int i = 0; i < schema.getFieldCount(); i++) {
            ByteString value = valueReader.readString();
            if (value == null) {
                return null;
            }

            if (i == schema.getFieldCount() - 1 && !valueReader.isEndWithNewLine()) {
                throw new IOException("bad format: expect new-line");
            } else if (i < schema.getFieldCount() - 1 && !valueReader.isEndWithSeparator()) {
                throw new IOException("bad format: expect separator");
            }
            values[i] = value;
            valueReader.reset();
        }
        return new Row(values);
    }

    public void close() throws DataSourceException {
        try {
            this.in.close();
        } catch (IOException ex) {
            throw new DataSourceException(ex);
        }
    }
}


