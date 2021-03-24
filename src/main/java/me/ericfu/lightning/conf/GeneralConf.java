package me.ericfu.lightning.conf;

import org.hibernate.validator.constraints.Range;

public final class GeneralConf extends Conf {

    @Range(min = 1, max = 10000000)
    private int batchSize = 10000;

    @Range(min = 1, max = 1024)
    private int threads = 16;

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    public int getThreads() {
        return threads;
    }

    public void setThreads(int threads) {
        this.threads = threads;
    }
}
