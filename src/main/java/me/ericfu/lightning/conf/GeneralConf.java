package me.ericfu.lightning.conf;

import me.ericfu.lightning.exception.InvalidConfigException;

public final class GeneralConf implements Conf {

    private int batchSize = 10000;

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

    @Override
    public void validate() throws InvalidConfigException {
        // do nothing
    }
}
