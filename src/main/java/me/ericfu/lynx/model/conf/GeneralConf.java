package me.ericfu.lynx.model.conf;

import lombok.Data;
import org.hibernate.validator.constraints.Range;

import javax.annotation.Nullable;
import javax.validation.constraints.Positive;

@Data
public final class GeneralConf {

    @Range(min = 1, max = 10000000)
    private int batchSize = 10000;

    @Range(min = 1, max = 1024)
    private int threads = 16;

    /**
     * Path of checkpoint file. Leave it unset to disable checkpointing
     */
    @Nullable
    private String checkpointFile;

    /**
     * Checkpoint interval in seconds
     */
    @Positive
    private int checkpointInterval = 5;
}
