package me.ericfu.lynx.sink.text;

import com.google.common.io.CountingOutputStream;
import lombok.Data;
import me.ericfu.lynx.data.ByteArray;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.model.checkpoint.SinkCheckpoint;
import me.ericfu.lynx.schema.type.BasicType;
import me.ericfu.lynx.sink.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;

public class TextSinkWriter implements SinkWriter {

    private static final Logger logger = LoggerFactory.getLogger(TextSinkWriter.class);

    private final TextSink s;
    private final File file;
    private final TextSinkTable table;
    private final byte[] sep;
    private final Charset charset;

    private CountingOutputStream out;
    private long startOffset;
    private long lastBatchOffset;

    public TextSinkWriter(TextSink s, File file, TextSinkTable table, Charset charset) {
        this.s = s;
        this.file = file;
        this.table = table;
        this.sep = s.conf.getSeparator().getBytes(StandardCharsets.UTF_8);
        this.charset = charset;
    }

    @Override
    public void open(SinkCheckpoint checkpoint) throws DataSinkException {
        final Checkpoint cp = (Checkpoint) checkpoint;
        long offset = cp == null ? 0 : cp.fileOffset;

        // Create upper-level directory
        synchronized (s) {
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new DataSinkException("cannot create directory " + file.getParent());
                }
            }
        }
        if (offset == 0) {
            if (file.exists()) {
                throw new DataSinkException("File '" + file.getPath() + "' exists");
            }
        } else {
            if (!file.exists()) {
                throw new DataSinkException("File '" + file.getPath() + "' not exists");
            }
        }

        FileOutputStream fos;
        try {
            fos = new FileOutputStream(file, offset > 0);
        } catch (FileNotFoundException e) {
            throw new DataSinkException("cannot open file", e);
        }

        if (offset > 0) {
            try {
                fos.getChannel().truncate(offset);
            } catch (IOException e) {
                throw new DataSinkException("cannot truncate to checkpoint offset", e);
            }
        }

        this.out = new CountingOutputStream(new BufferedOutputStream(fos));
        this.startOffset = offset;
    }

    @Override
    public void writeBatch(RecordBatch batch) throws DataSinkException {
        for (Record record : batch) {
            try {
                writeRecord(record);
            } catch (IOException e) {
                throw new DataSinkException(e);
            }
        }

        try {
            out.flush();
        } catch (IOException e) {
            throw new DataSinkException("flush failed", e);
        }

        this.lastBatchOffset = startOffset + out.getCount();
    }

    private void writeRecord(Record record) throws IOException {
        assert table.getType().getFieldCount() == record.size();
        for (int i = 0; i < table.getType().getFieldCount(); i++) {
            final Object v = record.getValue(i);
            if (v != null) {
                BasicType type = table.getType().getField(i).getType();
                if (type == BasicType.STRING || type == BasicType.BINARY) {
                    writeVariableLength(type, v);
                } else {
                    writeFixedLength(type, v);
                }
            }
            if (i < table.getType().getFieldCount() - 1) {
                out.write(sep);
            } else {
                out.write('\n');
            }
        }
    }

    private void writeVariableLength(BasicType type, Object v) throws IOException {
        out.write('"');
        switch (type) {
        case STRING:
            out.write(((String) v).replace("\"", "\"\"").getBytes(charset));
            break;
        case BINARY:
            for (byte b : ((ByteArray) v).getBytes()) {
                if (b == '"') {
                    out.write(b);
                    out.write(b);
                } else {
                    out.write(b);
                }
            }
            break;
        default:
            throw new AssertionError();
        }
        out.write('"');
    }

    private void writeFixedLength(BasicType type, Object v) throws IOException {
        String text;
        switch (type) {
        case BOOLEAN:
            text = Boolean.toString((Boolean) v);
            break;
        case INT:
            text = Integer.toString((Integer) v);
            break;
        case LONG:
            text = Long.toString((Long) v);
            break;
        case FLOAT:
            text = Float.toString((Float) v);
            break;
        case DOUBLE:
            text = Double.toString((Double) v);
            break;
        default:
            throw new AssertionError();
        }
        out.write(text.getBytes(StandardCharsets.ISO_8859_1));
    }

    @Override
    public void close() throws DataSinkException {
        try {
            if (out != null) {
                out.close();
            }
        } catch (IOException e) {
            throw new DataSinkException("cannot close file", e);
        }
    }

    @Override
    public SinkCheckpoint checkpoint() {
        Checkpoint cp = new Checkpoint();
        cp.setFileOffset(lastBatchOffset);
        return cp;
    }

    @Data
    public static class Checkpoint implements SinkCheckpoint {
        private long fileOffset;
    }
}
