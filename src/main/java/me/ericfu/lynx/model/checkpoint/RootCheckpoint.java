package me.ericfu.lynx.model.checkpoint;

import lombok.Data;
import me.ericfu.lynx.pipeline.Task;

import java.util.Date;
import java.util.List;
import java.util.Map;

@Data
public class RootCheckpoint {

    private Date timestamp = new Date();

    /**
     * Checkpoints of partitions of each table
     */
    private Map<String, List<Task.Checkpoint>> checkpoints;

}
