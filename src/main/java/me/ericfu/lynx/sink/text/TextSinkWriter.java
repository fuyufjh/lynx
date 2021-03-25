package me.ericfu.lynx.sink.text;

import me.ericfu.lynx.data.ByteArray;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.exception.DataSinkException;
import me.ericfu.lynx.schema.BasicType;
import me.ericfu.lynx.schema.Table;
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
    private final Table table;
    private final byte[] sep;
    private final Charset charset;

    private BufferedOutputStream out;

    public TextSinkWriter(TextSink s, File file, Table table, Charset charset) {
        this.s = s;
        this.file = file;
        this.table = table;
        this.sep = s.conf.getSeparator().getBytes(StandardCharsets.UTF_8);
        this.charset = charset;
    }

    @Override
    public void open() throws DataSinkException {
        // Create upper-level directory
        synchronized (s) {
            if (!file.getParentFile().exists()) {
                if (!file.getParentFile().mkdirs()) {
                    throw new DataSinkException("cannot create directory " + file.getParent());
                }
            }
        }
        if (file.exists()) {
            throw new DataSinkException("File '" + file.getPath() + "' exists");
        }
        try {
            out = new BufferedOutputStream(new FileOutputStream(file));
        } catch (FileNotFoundException e) {
            throw new DataSinkException("cannot open file", e);
        }
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
}
