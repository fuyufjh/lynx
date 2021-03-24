package me.ericfu.lightning.source.random;

import lombok.Data;
import me.ericfu.lightning.conf.SourceConf;

import javax.validation.constraints.Positive;

@Data
public class RandomSourceConf implements SourceConf {

    /**
     * Total number of records
     */
    @Positive
    private int records = 10000;

    /**
     * Auto-increment columns will be filled with auto-increment integer values instead of random
     */
    private String autoIncrementKey = "";

}
