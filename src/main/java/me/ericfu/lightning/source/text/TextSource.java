package me.ericfu.lightning.source.text;

import com.google.common.base.Preconditions;
import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.data.ByteString;
import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.data.RecordBatchBuilder;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.schema.BasicType;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.schema.RecordTypeBuilder;
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
    private RecordBatchBuilder builder;

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
        builder = new RecordBatchBuilder(globals.getBatchSize());
    }

    private void openSingleFile(File file) throws IOException {
        this.in = new BufferedInputStream(new FileInputStream(file));
    }

    public RecordBatch readBatch() throws DataSourceException {
        while (builder.size() < globals.getBatchSize()) {
            Record record;
            try {
                record = readNextRow();
            } catch (IOException ex) {
                throw new DataSourceException(ex);
            }
            if (record == null) {
                break;
            }
            builder.addRow(record);
        }
        if (builder.size() > 0) {
            return builder.buildAndReset();
        } else {
            return null;
        }
    }

    private Record readNextRow() throws IOException {
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
        return new Record(schema, values);
    }

    public void close() throws DataSourceException {
        try {
            this.in.close();
        } catch (IOException ex) {
            throw new DataSourceException(ex);
        }
    }
}


