package me.ericfu.lynx.pipeline;

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
            saveCheckpoint();
        } catch (Exception e) {
            logger.error("Checkpoint failed", e);
            // notify other tasks to stop
            fatalError.compareAndSet(null, e);
        }
    }

    public synchronized void saveCheckpoint() throws CheckpointException {
        RootCheckpoint cp = new RootCheckpoint();
        cp.setCheckpoints(taskboard.getSchemaPipelines().entrySet().stream().collect(Collectors.toMap(
            e -> e.getKey(),
            e -> e.getValue().stream().map(Pipeline::getCheckpoint).collect(Collectors.toList())
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
            List<Pipeline.Checkpoint> checkpoints = root.getCheckpoints().get(schema);
            List<Pipeline> pipelines = taskboard.getSchemaPipelines().get(schema);
            if (pipelines.isEmpty()) {
                throw new CheckpointException("unknown schema '" + schema + "' in checkpoint");
            }
            if (pipelines.size() != checkpoints.size()) {
                throw new CheckpointException("number of pipelines mismatch");
            }
            for (int i = 0; i < checkpoints.size(); i++) {
                pipelines.get(i).setCheckpoint(checkpoints.get(i));
            }
        }
    }
}
