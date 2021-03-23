package me.ericfu.lightning;

import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.schema.RecordBatchConvertor;
import me.ericfu.lightning.sink.SinkWriter;
import me.ericfu.lightning.source.SourceReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipeline implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    private final int no;
    private final SourceReader source;
    private final SinkWriter sink;
    private final RecordBatchConvertor convertor;

    public Pipeline(int no, SourceReader source, SinkWriter sink, RecordBatchConvertor convertor) {
        this.no = no;
        this.source = source;
        this.sink = sink;
        this.convertor = convertor;
    }

    @Override
    public void run() {
        logger.info("Pipeline {} started", no);
        long count;
        try {
            count = transfer();
        } catch (Exception ex) {
            logger.error("Error during data transferring", ex);
            return;
        }
        logger.info("Pipeline {} completed: {} rows transferred", no, count);
    }

    private long transfer() throws Exception {
        try {
            source.open();
            sink.open();

            long count = 0;
            RecordBatch batch;
            while ((batch = source.readBatch()) != null) {
                count += batch.size();
                batch = convertor.convert(batch);
                sink.writeBatch(batch);
            }
            return count;

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
}
