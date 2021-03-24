package me.ericfu.lightning.sink.text;

import me.ericfu.lightning.data.ByteString;
import me.ericfu.lightning.data.Record;
import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.schema.Table;
import me.ericfu.lightning.sink.SinkWriter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.charset.StandardCharsets;

public class TextSinkWriter implements SinkWriter {

    private static final Logger logger = LoggerFactory.getLogger(TextSinkWriter.class);

    private final TextSink s;
    private final File file;
    private final Table table;
    private final byte[] sep;

    private BufferedOutputStream out;

    public TextSinkWriter(TextSink s, File file, Table table) {
        this.s = s;
        this.file = file;
        this.table = table;
        this.sep = s.conf.getSeparator().getBytes(StandardCharsets.UTF_8);
    }

    @Override
    public void open() throws DataSinkException {
        if (file.exists()) {
            logger.warn("File '" + file.getPath() + "' exists. Original content will be cleared");
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
                String text;
                switch (table.getType().getField(i).getType()) {
                case INT64:
                    text = Long.toString((Long) v);
                    break;
                case FLOAT:
                    text = Float.toString((Float) v);
                    break;
                case DOUBLE:
                    text = Double.toString((Double) v);
                    break;
                case STRING:
                    text = ((ByteString) v).toString().replace("\"", "\"\"");
                    break;
                default:
                    throw new AssertionError("unreachable");
                }
                out.write(text.getBytes(StandardCharsets.UTF_8));
            }
            if (i < table.getType().getFieldCount() - 1) {
                out.write(sep);
            } else {
                out.write('\n');
            }
        }
    }

    @Override
    public void close() throws DataSinkException {
        try {
            out.close();
        } catch (IOException e) {
            throw new DataSinkException("cannot close file", e);
        }
    }
}
