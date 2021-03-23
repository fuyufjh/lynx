package me.ericfu.lightning.source.random;

import me.ericfu.lightning.conf.Kind;
import me.ericfu.lightning.conf.SourceConf;
import me.ericfu.lightning.exception.InvalidConfigException;

@Kind("random")
public class RandomSourceConf extends SourceConf {

    /**
     * Total number of records
     */
    private int records = 10000;

    /**
     * Auto-increment columns will be filled with auto-increment integer values instead of random
     */
    private String autoIncrementKey = "";

    public int getRecords() {
        return records;
    }

    public void setRecords(int records) {
        this.records = records;
    }

    public String getAutoIncrementKey() {
        return autoIncrementKey;
    }

    public void setAutoIncrementKey(String autoIncrementKey) {
        this.autoIncrementKey = autoIncrementKey;
    }

    @Override
    public void validate() throws InvalidConfigException {
        // do nothing
    }
}
