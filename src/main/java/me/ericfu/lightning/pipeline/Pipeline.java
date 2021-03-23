package me.ericfu.lightning.pipeline;

import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.schema.RecordBatchConvertor;
import me.ericfu.lightning.sink.SinkWriter;
import me.ericfu.lightning.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Pipeline is composed by a pair of source and sink as well as the related utilities, such as data type convertor.
 * By design a pipeline is always executed by a single thread.
 */
public class Pipeline implements Callable<PipelineResult> {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    private final int no;
    private final SourceReader source;
    private final SinkWriter sink;
    private final RecordBatchConvertor convertor;
    private final AtomicReference<Throwable> fatalError;

    public Pipeline(int no, SourceReader source, SinkWriter sink, RecordBatchConvertor convertor,
                    AtomicReference<Throwable> fatalError) {
        this.no = no;
        this.source = source;
        this.sink = sink;
        this.convertor = convertor;
        this.fatalError = fatalError;
    }

    @Override
    public PipelineResult call() throws Exception {
        try {
            source.open();
            sink.open();

            long count = 0;
            RecordBatch batch;
            while (checkFatalError() && (batch = source.readBatch()) != null) {
                count += batch.size();
                batch = convertor.convert(batch);
                sink.writeBatch(batch);
            }
            return new PipelineResult(count);

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
}
