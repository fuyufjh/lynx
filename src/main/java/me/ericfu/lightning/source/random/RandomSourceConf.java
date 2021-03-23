package me.ericfu.lightning.source.random;

import me.ericfu.lightning.conf.SourceConf;
import me.ericfu.lightning.exception.InvalidConfigException;

public class RandomSourceConf extends SourceConf {

    /**
     * Total number of records
     */
    private int records = 10000;

    public int getRecords() {
        return records;
    }

    public void setRecords(int records) {
        this.records = records;
    }

    @Override
    public void validate() throws InvalidConfigException {
        // do nothing
    }
}
