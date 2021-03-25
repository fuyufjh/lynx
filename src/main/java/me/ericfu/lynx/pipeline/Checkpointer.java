package me.ericfu.lynx.pipeline;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import me.ericfu.lynx.exception.CheckpointException;
import me.ericfu.lynx.model.checkpoint.RootCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Checkpointer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Checkpointer.class);

    private static final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    private final TaskBoard taskboard;
    private final File file;
    private final AtomicReference<Throwable> fatalError;

    public Checkpointer(TaskBoard taskboard, File file, AtomicReference<Throwable> fatalError) {
        this.taskboard = taskboard;
        this.file = file;
        this.fatalError = fatalError;
    }

    @Override
    public void run() {
        if (fatalError.get() != null) {
            return;
        }

        try {
            doCheckpoint();
        } catch (Exception e) {
            logger.error("Checkpoint failed", e);
            // notify other tasks to stop
            fatalError.compareAndSet(null, new CheckpointException(e));
        }
    }

    private synchronized void doCheckpoint() throws IOException {
        RootCheckpoint cp = new RootCheckpoint();
        cp.setCheckpoints(taskboard.getSchemaPipelines().entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue().stream().map(Pipeline::checkpoint).collect(Collectors.toList())
        )));

        // Write into a temp file and then rename can ensure atomicity
        File tempFile = File.createTempFile("temp-", ".lynx", file.getParentFile());

        mapper.writeValue(tempFile, cp);
        if (!tempFile.renameTo(file)) {
            throw new IOException("failed to rename '" + tempFile + "' to '" + file + "'");
        }
    }
}
