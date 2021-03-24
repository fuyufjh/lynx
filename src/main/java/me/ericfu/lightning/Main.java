package me.ericfu.lightning;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.ericfu.lightning.conf.RootConf;
import me.ericfu.lightning.exception.IncompatibleSchemaException;
import me.ericfu.lightning.pipeline.Pipeline;
import me.ericfu.lightning.pipeline.PipelineResult;
import me.ericfu.lightning.schema.RecordConvertor;
import me.ericfu.lightning.schema.Schema;
import me.ericfu.lightning.schema.SchemaUtils;
import me.ericfu.lightning.schema.Table;
import me.ericfu.lightning.sink.SchemalessSink;
import me.ericfu.lightning.sink.Sink;
import me.ericfu.lightning.sink.SinkFactory;
import me.ericfu.lightning.sink.SinkWriter;
import me.ericfu.lightning.source.SchemalessSource;
import me.ericfu.lightning.source.Source;
import me.ericfu.lightning.source.SourceFactory;
import me.ericfu.lightning.source.SourceReader;
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
            formatter.printHelp("lightning", options);
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
         * Execute Pipelines in Multi-Threads
         *---------------------------------------------------------*/

        ThreadPoolExecutor threadPool = new ThreadPoolExecutor(
            conf.getGeneral().getThreads(),
            conf.getGeneral().getThreads(),
            1, TimeUnit.SECONDS,
            new LinkedBlockingQueue<>(),
            new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Pipeline-%d").build());

        logger.info("Thread pool initialized. {} threads at most", conf.getGeneral().getThreads());

        // fatalError also helps stop all threads when a fatal error happens on one of the threads
        AtomicReference<Throwable> fatalError = new AtomicReference<>();
        List<Future<PipelineResult>> futures = new ArrayList<>();

        // Create pipelines for each table in data source
        for (Table sourceTable : sourceSchema.getTables()) {
            final Table sinkTable = sinkSchema.getTable(sourceTable.getName());
            assert sinkTable != null; // already checked

            // Num of pipeline is determined by num of source partitions
            Iterable<SourceReader> readers = source.createReaders(sourceTable);
            RecordConvertor convertor = new RecordConvertor(sourceTable.getType(), sinkTable.getType());

            int count = 0;
            for (SourceReader reader : readers) {
                SinkWriter writer = sink.createWriter(sinkTable);
                Pipeline task = new Pipeline(count++, reader, writer, convertor, fatalError);
                futures.add(threadPool.submit(task));
            }
            logger.info("{} pipelines created for table {}", count, sourceTable.getName());
        }

        logger.info("All pipelines created");

        List<PipelineResult> results = new ArrayList<>();
        for (Future<PipelineResult> future : futures) {
            try {
                results.add(future.get());
            } catch (InterruptedException | ExecutionException ex) {
                logger.error("Fatal error", ex);
                return;
            }
        }

        long totalRecords = results.stream().mapToLong(PipelineResult::getRecords).sum();
        logger.info("All pipelines completed. {} records transferred in total", totalRecords);

        threadPool.shutdown();
        logger.info("Bye!");
    }
}
