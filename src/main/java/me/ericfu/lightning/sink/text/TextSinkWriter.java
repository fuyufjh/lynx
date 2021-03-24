package me.ericfu.lightning.sink.text;

import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.exception.DataSinkException;
import me.ericfu.lightning.sink.SinkWriter;

import java.io.File;

public class TextSinkWriter implements SinkWriter {

    private final TextSink s;
    private final File file;

    public TextSinkWriter(TextSink s, File file) {
        this.s = s;
        this.file = file;
    }

    @Override
    public void open() throws DataSinkException {

    }

    @Override
    public void writeBatch(RecordBatch batch) throws DataSinkException {

    }

    @Override
    public void close() throws DataSinkException {

    }
}
