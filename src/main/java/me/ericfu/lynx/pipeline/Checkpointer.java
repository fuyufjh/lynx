package me.ericfu.lynx.pipeline;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import me.ericfu.lynx.exception.CheckpointException;
import me.ericfu.lynx.model.checkpoint.RootCheckpoint;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

public class Checkpointer implements Runnable {

    private static final Logger logger = LoggerFactory.getLogger(Checkpointer.class);

    private static final ObjectMapper mapper = new ObjectMapper()
        .enable(SerializationFeature.INDENT_OUTPUT)
        .setSerializationInclusion(JsonInclude.Include.NON_NULL);

    private final Pipeline pipeline;
    private final File file;
    private final AtomicReference<Throwable> fatalError;

    public Checkpointer(Pipeline pipeline, File file, AtomicReference<Throwable> fatalError) {
        this.pipeline = pipeline;
        this.file = file;
        this.fatalError = fatalError;
    }

    @Override
    public void run() {
        if (fatalError.get() != null) {
            return;
        }

        try {
            saveCheckpoint();
        } catch (Exception e) {
            logger.error("Checkpoint failed", e);
            // notify other tasks to stop
            fatalError.compareAndSet(null, e);
        }
    }

    public synchronized void saveCheckpoint() throws CheckpointException {
        RootCheckpoint cp = new RootCheckpoint();
        cp.setCheckpoints(pipeline.getTableTasks().entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue().stream().map(Task::getCheckpoint).collect(Collectors.toList())
        )));

        try {
            // Write into a temp file and then rename can ensure atomicity
            File tempFile = File.createTempFile("temp-", ".lynx", file.getParentFile());

            mapper.writeValue(tempFile, cp);
            if (!tempFile.renameTo(file)) {
                throw new IOException("failed to rename '" + tempFile + "' to '" + file + "'");
            }
        } catch (IOException ex) {
            throw new CheckpointException(ex);
        }
    }

    public synchronized void loadCheckpoint() throws CheckpointException {
        if (!file.exists()) {
            return;
        }

        RootCheckpoint root;
        try {
            root = mapper.readValue(file, RootCheckpoint.class);
        } catch (IOException e) {
            throw new CheckpointException(e);
        }

        for (String schema : root.getCheckpoints().keySet()) {
            List<Task.Checkpoint> checkpoints = root.getCheckpoints().get(schema);
            List<Task> tasks = pipeline.getTableTasks().get(schema);
            if (tasks.isEmpty()) {
                throw new CheckpointException("unknown schema '" + schema + "' in checkpoint");
            }
            if (tasks.size() != checkpoints.size()) {
                throw new CheckpointException("number of tasks mismatch");
            }
            for (int i = 0; i < checkpoints.size(); i++) {
                tasks.get(i).setCheckpoint(checkpoints.get(i));
            }
        }
    }
}
