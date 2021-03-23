package me.ericfu.lightning.conf;

import me.ericfu.lightning.exception.InvalidConfigException;

public final class GeneralConf implements Conf {

    private int batchSize = 10000;

    public void setBatchSize(int batchSize) {
        this.batchSize = batchSize;
    }

    public int getBatchSize() {
        return batchSize;
    }

    @Override
    public void validate() throws InvalidConfigException {
        // do nothing
    }
}
