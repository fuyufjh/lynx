package me.ericfu.lightning.source.text;

import me.ericfu.lightning.data.ByteString;
import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.data.RecordBatchBuilder;
import me.ericfu.lightning.exception.DataSourceException;
import me.ericfu.lightning.source.SourceReader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class TextSourceReader implements SourceReader {

    private final TextSource s;
    private final int partNo;

    private BufferedInputStream in;
    private TextValueReader valueReader;
    private RecordBatchBuilder builder;

    public TextSourceReader(TextSource s, int partNo) {
        this.s = s;
        this.partNo = partNo;
    }

    public void open() throws DataSourceException {
        if (partNo > 0) {
            return;
        }

        if (s.conf.getSeparator().length() == 1 && s.conf.getSeparator().charAt(0) < 0x80) {
            s.sep = (byte) s.conf.getSeparator().charAt(0);
        } else {
            throw new DataSourceException("bad separator '" + s.conf.getSeparator() + "'");
        }

        final File file = new File(s.conf.getPath());
        if (!file.exists()) {
            throw new DataSourceException("file or directory '" + s.conf.getPath() + "' not exist");
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

        valueReader = new TextValueReader(in, s.sep);
        builder = new RecordBatchBuilder(s.globals.getBatchSize());
    }

    private void openSingleFile(File file) throws IOException {
        this.in = new BufferedInputStream(new FileInputStream(file));
    }

    public RecordBatch readBatch() throws DataSourceException {
        if (partNo > 0) {
            return null;
        }

        while (builder.size() < s.globals.getBatchSize()) {
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
        Object[] values = new Object[s.schema.getFieldCount()];
        for (int i = 0; i < s.schema.getFieldCount(); i++) {
            ByteString value = valueReader.readString();
            if (value == null) {
                return null;
            }

            if (i == s.schema.getFieldCount() - 1 && !valueReader.isEndWithNewLine()) {
                throw new IOException("bad format: expect new-line");
            } else if (i < s.schema.getFieldCount() - 1 && !valueReader.isEndWithSeparator()) {
                throw new IOException("bad format: expect separator");
            }
            values[i] = value;
            valueReader.reset();
        }
        return new Record(s.schema, values);
    }

    public void close() throws DataSourceException {
        if (partNo > 0) {
            return;
        }

        try {
            this.in.close();
        } catch (IOException ex) {
            throw new DataSourceException(ex);
        }
    }
}
