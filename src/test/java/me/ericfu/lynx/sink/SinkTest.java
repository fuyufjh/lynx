package me.ericfu.lynx.sink;

import me.ericfu.lynx.PluginTest;
import me.ericfu.lynx.data.Record;
import me.ericfu.lynx.model.checkpoint.SinkCheckpoint;
import me.ericfu.lynx.schema.Table;

public abstract class SinkTest extends PluginTest {

    protected void writeAllRecords(Sink sink, Table table) {
        writeAllRecords(sink, table, null);
    }

    protected void writeAllRecords(Sink sink, Table table, SinkCheckpoint cp) {
        SinkWriter writer = sink.createWriter(table);

    }

    /**
     * Written record with addition information to check writer's behaviour
     */
    protected static class WriteResult {
        public final Record record;
        public final int batch;
        public final int index;

        public WriteResult(Record record, int batch, int index) {
            this.record = record;
            this.batch = batch;
            this.index = index;
        }
    }
}
