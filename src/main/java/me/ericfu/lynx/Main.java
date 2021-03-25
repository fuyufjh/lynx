package me.ericfu.lynx;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.ericfu.lynx.exception.IncompatibleSchemaException;
import me.ericfu.lynx.model.conf.RootConf;
import me.ericfu.lynx.pipeline.Checkpointer;
import me.ericfu.lynx.pipeline.Pipeline;
import me.ericfu.lynx.pipeline.PipelineResult;
import me.ericfu.lynx.pipeline.TaskBoard;
import me.ericfu.lynx.schema.RecordConvertor;
import me.ericfu.lynx.schema.Schema;
import me.ericfu.lynx.schema.SchemaUtils;
import me.ericfu.lynx.schema.Table;
import me.ericfu.lynx.sink.SchemalessSink;
import me.ericfu.lynx.sink.Sink;
import me.ericfu.lynx.sink.SinkFactory;
import me.ericfu.lynx.sink.SinkWriter;
import me.ericfu.lynx.source.SchemalessSource;
import me.ericfu.lynx.source.Source;
import me.ericfu.lynx.source.SourceFactory;
import me.ericfu.lynx.source.SourceReader;
import org.apache.commons.cli.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicReference;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {

        /*----------------------------------------------------------
         * Parse Command-line Options
         *---------------------------------------------------------*/

        Options options = new Options();
        options.addRequiredOption("c", "conf", true, "path to config file");

        CommandLineParser parser = new DefaultParser();
        HelpFormatter formatter = new HelpFormatter();

        CommandLine cmd;
        try {
            cmd = parser.parse(options, args);
        } catch (Exception e) {
            logger.error("Failed to parse arguments", e);
            formatter.printHelp("lynx", options);
            return;
        }

        String confPath = cmd.getOptionValue("c");
        if (!new File(confPath).exists()) {
            logger.error("File {} not exist", confPath);
            return;
        }

        /*----------------------------------------------------------
         * Load Configurations
         *---------------------------------------------------------*/

        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        RootConf conf;
        try {
            conf = mapper.readValue(new File(confPath), RootConf.class);
            conf.validate();
        } catch (Exception e) {
            logger.error("Invalid config: {}", e.getMessage());
            return;
        }

        logger.info("Loaded config from {}", confPath);

        /*----------------------------------------------------------
         * Construct and Initialize Source and Sink
         *---------------------------------------------------------*/

        Source source = new SourceFactory().create(conf.getGeneral(), conf.getSource());
        Sink sink = new SinkFactory().create(conf.getGeneral(), conf.getSink());

        try {
            source.init();
            sink.init();
        } catch (Exception ex) {
            logger.error("Initialize source or sink failed", ex);
            return;
        }

        /*----------------------------------------------------------
         * Decide Schema and Types
         *---------------------------------------------------------*/

        Schema sourceSchema;
        Schema sinkSchema;
        if (source instanceof SchemalessSource && sink instanceof SchemalessSink) {
            logger.error("At least one of source or sink should be with schema");
            return;
        } else if (source instanceof SchemalessSource) {
            logger.info("Data source is schemaless. Provide schema of sink to it");
            sinkSchema = sink.getSchema();
            ((SchemalessSource) source).provideSchema(sinkSchema);
            sourceSchema = source.getSchema();
        } else if (sink instanceof SchemalessSink) {
            logger.info("Data sink is schemaless. Provide schema of source to it");
            sourceSchema = source.getSchema();
            ((SchemalessSink) sink).provideSchema(sourceSchema);
            sinkSchema = sink.getSchema();
        } else {
            sourceSchema = source.getSchema();
            sinkSchema = sink.getSchema();
        }

        logger.info("Data source schema: " + sourceSchema.toString());
        logger.info("Data sink schema: " + sinkSchema.toString());

        try {
            SchemaUtils.checkCompatible(sourceSchema, sinkSchema);
        } catch (IncompatibleSchemaException e) {
            logger.error("Schema not compatible: {}", e.getMessage());
        }

        /*----------------------------------------------------------
         * Prepare All Pipelines
         *---------------------------------------------------------*/

        // fatalError also helps stop all threads when a fatal error happens on one of the threads
        AtomicReference<Throwable> fatalError = new AtomicReference<>();
        List<Future<PipelineResult>> futures = new ArrayList<>();

        TaskBoard.Builder taskBoardBuilder = new TaskBoard.Builder();

        // Create pipelines for each table in data source
        for (Table sourceTable : sourceSchema.getTables()) {
            final Table sinkTable = sinkSchema.getTable(sourceTable.getName());
            assert sinkTable != null; // already checked
            taskBoardBuilder.setCurrentTable(sinkTable.getName());

            // Num of pipeline is determined by num of source partitions
            List<SourceReader> readers = source.createReaders(sourceTable);
            RecordConvertor convertor = new RecordConvertor(sourceTable.getType(), sinkTable.getType());

            int count = 0;
            for (SourceReader reader : readers) {
                SinkWriter writer = sink.createWriter(sinkTable);
                Pipeline pipeline = new Pipeline(sourceTable.getName(), count++, reader, writer, convertor, fatalError);
                taskBoardBuilder.addPipeline(pipeline);
            }
            logger.info("Table {}: {} pipelines created", sourceTable.getName(), count);
        }

        TaskBoard taskBoard = taskBoardBuilder.build();
        logger.info("All pipelines created");

        /*----------------------------------------------------------
         * Execute Pipelines in Parallel
         *---------------------------------------------------------*/

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
            conf.getGeneral().getThreads(),
            conf.getGeneral().getThreads(),
            1, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Pipeline-%d").build());

        ScheduledThreadPoolExecutor scheduledExecutor = new ScheduledThreadPoolExecutor(2,
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Scheduled-%d").build());

        logger.info("Thread pool initialized. {} threads at most", conf.getGeneral().getThreads());

        Checkpointer checkpointer;
        if (conf.getGeneral().getCheckpointFile() != null) {
            File checkpointFile = new File(conf.getGeneral().getCheckpointFile());
            int interval = conf.getGeneral().getCheckpointInterval();
            checkpointer = new Checkpointer(taskBoard, checkpointFile, fatalError);
            scheduledExecutor.scheduleAtFixedRate(checkpointer, interval, interval, TimeUnit.SECONDS);
            logger.info("Checkpoint task scheduled");
        } else {
            checkpointer = null;
            logger.warn("Checkpoint is disabled");
        }

        for (Pipeline pipeline : taskBoard.getPipelines()) {
            futures.add(threadPool.submit(pipeline));
        }

        List<PipelineResult> results = new ArrayList<>();
        for (Future<PipelineResult> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException | ExecutionException ex) {
                logger.error("Fatal error", ex.getCause());
            }
        }

        if (checkpointer != null) {
            checkpointer.run();
        }

        long totalRecords = results.stream().mapToLong(PipelineResult::getRecords).sum();
        if (fatalError.get() == null) {
            logger.info("All pipelines completed. {} records transferred in total", totalRecords);
        } else {
            logger.error("All pipeline may not complete since some fatal error happened. {} records transferred", totalRecords);
        }

        threadPool.shutdown();
    }
}
