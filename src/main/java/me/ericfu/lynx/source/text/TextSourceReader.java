package me.ericfu.lynx.source.text;

import com.google.common.io.CountingInputStream;
import lombok.Data;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.data.RecordBatchBuilder;
import me.ericfu.lynx.exception.DataSourceException;
import me.ericfu.lynx.model.checkpoint.SourceCheckpoint;
import me.ericfu.lynx.schema.type.StructType;
import me.ericfu.lynx.source.SourceReader;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

public class TextSourceReader implements SourceReader {

    private final TextSource s;
    private final StructType type;

    /**
     * file must be a normal file, not a directory
     */
    private final File file;

    private CountingInputStream in;
    private TextValueReader valueReader;
    private RecordBatchBuilder builder;

    public TextSourceReader(TextSource s, StructType type, File file) {
        this.s = s;
        this.type = type;
        this.file = file;
    }

    public void open() throws DataSourceException {
        if (s.conf.getSeparator().length() == 1 && s.conf.getSeparator().charAt(0) < 0x80) {
            s.sep = (byte) s.conf.getSeparator().charAt(0);
        } else {
            throw new DataSourceException("bad separator '" + s.conf.getSeparator() + "'");
        }

        if (!file.exists()) {
            throw new DataSourceException("file or directory '" + s.conf.getPath() + "' not exist");
        }

        try {
            this.in = new CountingInputStream(new BufferedInputStream(new FileInputStream(file)));
        } catch (IOException ex) {
            throw new DataSourceException(ex);
        }

        valueReader = new TextValueReader(in, s.charset, s.sep);
        builder = new RecordBatchBuilder(s.globals.getBatchSize());
    }

    @Override
    public void open(SourceCheckpoint checkpoint) throws DataSourceException {
        open();

        Checkpoint cp = (Checkpoint) checkpoint;
        try {
            in.skip(cp.fileOffset);
        } catch (IOException e) {
            throw new DataSourceException("cannot seek to checkpoint offset " + cp.fileOffset);
        }
    }

    public RecordBatch readBatch() throws DataSourceException {
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
        Object[] values = new Object[type.getFieldCount()];
        for (int i = 0; i < type.getFieldCount(); i++) {
            String value = valueReader.readString();
            if (value == null) {
                return null;
            }

            if (i == type.getFieldCount() - 1 && !valueReader.isEndWithNewLine()) {
                throw new IOException("bad format: expect new-line");
            } else if (i < type.getFieldCount() - 1 && !valueReader.isEndWithSeparator()) {
                throw new IOException("bad format: expect separator");
            }
            values[i] = value;
            valueReader.reset();
        }
        return new Record(values);
    }

    public void close() throws DataSourceException {
        try {
            this.in.close();
        } catch (IOException ex) {
            throw new DataSourceException(ex);
        }
    }

    @Override
    public SourceCheckpoint checkpoint() {
        Checkpoint cp = new Checkpoint();
        cp.setFileOffset(in.getCount());
        return cp;
    }

    @Data
    public static class Checkpoint implements SourceCheckpoint {
        private long fileOffset;
    }
}
