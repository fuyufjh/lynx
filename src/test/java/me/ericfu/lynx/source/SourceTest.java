package me.ericfu.lynx.source;

import me.ericfu.lynx.PluginTest;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.data.RecordBatch;
import me.ericfu.lynx.model.checkpoint.SourceCheckpoint;
import me.ericfu.lynx.schema.Table;

import java.util.ArrayList;
import java.util.List;

public abstract class SourceTest extends PluginTest {

    /**
     * Read all records from source
     */
    protected List<ReadResult> readAllRecords(Source source, Table table) throws Exception {
        return readAllRecords(source, table, null);
    }

    /**
     * Read all records from source started with a checkpoint
     */
    protected List<ReadResult> readAllRecords(Source source, Table table,
                                              List<SourceCheckpoint> checkpoints) throws Exception {
        List<SourceReader> readers = source.createReaders(table);

        List<ReadResult> results = new ArrayList<>();
        for (int readerNo = 0; readerNo < readers.size(); readerNo++) {
            final SourceReader reader = readers.get(readerNo);
            reader.open(checkpoints != null ? checkpoints.get(readerNo) : null);

            RecordBatch batch;
            for (int batchNo = 0; (batch = reader.readBatch()) != null; batchNo++) {
                for (int recordNo = 0; recordNo < batch.size(); recordNo++) {
                    results.add(new ReadResult(batch.getRecord(recordNo), readerNo, batchNo, recordNo));
                }
            }

            reader.close();
        }
        return results;
    }

    /**
     * Record with additional info to check reader's behaviour
     */
    protected static class ReadResult {
        public final Record record;
        public final int reader;
        public final int batch;
        public final int index;

        public ReadResult(Record record, int reader, int batch, int index) {
            this.record = record;
            this.reader = reader;
            this.batch = batch;
            this.index = index;
        }
    }
}
