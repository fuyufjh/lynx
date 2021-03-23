package me.ericfu.lightning;

import me.ericfu.lightning.data.RecordBatch;
import me.ericfu.lightning.schema.RecordBatchConvertor;
import me.ericfu.lightning.sink.Sink;
import me.ericfu.lightning.source.Source;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Pipeline implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Pipeline.class);

    private final Source source;
    private final Sink sink;
    private final RecordBatchConvertor convertor;

    public Pipeline(Source source, Sink sink, RecordBatchConvertor convertor) {
        this.source = source;
        this.sink = sink;
        this.convertor = convertor;
    }

    @Override
    public void run() {
        long count;
        try {
            count = transfer();
        } catch (Exception ex) {
            logger.error("Error during data transferring", ex);
            return;
        }
        logger.info("Pipeline {}: {} rows transferred", 1, count);
    }

    private long transfer() throws Exception {
        long count = 0;
        RecordBatch batch;
        while ((batch = source.readBatch()) != null) {
            count += batch.size();
            batch = convertor.convert(batch);
            sink.writeBatch(batch);
        }
        return count;
    }
}
