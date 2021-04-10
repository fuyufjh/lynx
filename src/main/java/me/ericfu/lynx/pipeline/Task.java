package me.ericfu.lynx.pipeline;

import lombok.Data;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.model.checkpoint.SinkCheckpoint;
import me.ericfu.lynx.model.checkpoint.SourceCheckpoint;
import me.ericfu.lynx.schema.convert.RecordBatchConvertor;
import me.ericfu.lynx.schema.convert.RecordConvertor;
import me.ericfu.lynx.sink.SinkWriter;
import me.ericfu.lynx.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Task is composed by a pair of source and sink as well as the related utilities, such as data type convertor.
 * By design a task is always executed by a single thread.
 */
public class Task implements Callable<TaskResult> {

    private static final Logger logger = LoggerFactory.getLogger(Task.class);

    private final String name;
    private final int part;

    private final SourceReader source;
    private final SinkWriter sink;
    private final RecordBatchConvertor convertor;
    private final AtomicReference<Throwable> fatalError;

    private volatile Checkpoint checkpoint = new Checkpoint();

    public Task(String name, int part, SourceReader source, SinkWriter sink, RecordConvertor convertor,
                AtomicReference<Throwable> fatalError) {
        this.name = name;
        this.part = part;
        this.source = source;
        this.sink = sink;
        this.convertor = new RecordBatchConvertor(convertor);
        this.fatalError = fatalError;
    }

    @Override
    public TaskResult call() throws Exception {
        try {
            if (checkpoint.source == null) {
                source.open();
            } else {
                source.open(checkpoint.source);
            }

            if (checkpoint.sink == null) {
                sink.open();
            } else {
                sink.open(checkpoint.sink);
            }

            long count = 0;
            RecordBatch batch;
            while (checkFatalError() && (batch = source.readBatch()) != null) {
                count += batch.size();
                batch = convertor.convert(batch);
                sink.writeBatch(batch);

                // Update checkpoint after each batch
                Checkpoint cp = new Checkpoint();
                cp.setSource(source.checkpoint());
                cp.setSink(sink.checkpoint());
                this.checkpoint = cp;
            }
            return new TaskResult(count);

        } catch (Exception | Error ex) {
            // notify other pipeline to stop
            fatalError.compareAndSet(null, ex);
            throw ex;

        } finally {
            try {
                source.close();
            } catch (Exception ex) {
                logger.warn("close source failed", ex);
            }

            try {
                sink.close();
            } catch (Exception ex) {
                logger.warn("close sink failed", ex);
            }
        }
    }

    private boolean checkFatalError() {
        return fatalError.get() == null;
    }

    /**
     * Get a latest checkpoint from pipeline. By design this method may be called by checkpoint thread
     */
    public Checkpoint getCheckpoint() {
        return checkpoint;
    }

    /**
     * Set initial checkpoint before pipeline initialized
     */
    public void setCheckpoint(Checkpoint checkpoint) {
        this.checkpoint = checkpoint;
    }

    @Data
    public static class Checkpoint {
        private SourceCheckpoint source;
        private SinkCheckpoint sink;
    }
}
