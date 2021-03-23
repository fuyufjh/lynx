package me.ericfu.lightning;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.ericfu.lightning.exception.IncompatibleSchemaException;
import me.ericfu.lightning.exception.InvalidConfigException;
import me.ericfu.lightning.schema.BasicType;
import me.ericfu.lightning.schema.Field;
import me.ericfu.lightning.schema.RecordBatchConvertor;
import me.ericfu.lightning.schema.RecordConvertor;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.schema.RecordTypeBuilder;
import me.ericfu.lightning.sink.SchemalessSink;
import me.ericfu.lightning.sink.Sink;
import me.ericfu.lightning.sink.SinkFactory;
import me.ericfu.lightning.source.SchemalessSource;
import me.ericfu.lightning.source.Source;
import me.ericfu.lightning.source.SourceFactory;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

public class Main {

    private static final Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) {
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

        ConfigReader conf = new ConfigReader(new File(confPath));
        try {
            conf.readConfig();
        } catch (InvalidConfigException e) {
            logger.error("invalid config: {}", e.getMessage());
            return;
        }

        logger.info("Loaded config from {}", confPath);

        Source source = new SourceFactory().create(conf.getGeneralConf(), conf.getSourceConf());
        Sink sink = new SinkFactory().create(conf.getGeneralConf(), conf.getSinkConf());

        try {
            source.open();
            sink.open();
        } catch (Exception ex) {
            logger.error("Open source or sink failed", ex);
            return;
        }

        RecordType sourceSchema;
        RecordType sinkSchema;
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

        // Combine source and sink schema
        RecordType unifiedSchema;
        try {
            unifiedSchema = buildUnifiedSchema(sourceSchema, sinkSchema);
        } catch (IncompatibleSchemaException e) {
            logger.error("schema source and sink not compatible");
            return;
        }

        RecordConvertor recordConvertor = new RecordConvertor(sourceSchema, unifiedSchema);
        RecordBatchConvertor batchConvertor = new RecordBatchConvertor(recordConvertor);

        // Thread executors
        ThreadPoolExecutor threadPool =
            new ThreadPoolExecutor(1, 32, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("worker-%d").build());

        Future<?> future = threadPool.submit(new Pipeline(source, sink, batchConvertor));

        try {
            future.get();
        } catch (InterruptedException | ExecutionException ex) {
            logger.error("transfer failed", ex);
            return;
        }

        threadPool.shutdown();
        logger.info("Bye!");
    }

    private static RecordType buildUnifiedSchema(RecordType source, RecordType sink)
        throws IncompatibleSchemaException {
        if (source.getFieldCount() != sink.getFieldCount()) {
            throw new IncompatibleSchemaException("field counts not matches");
        }

        RecordTypeBuilder builder = new RecordTypeBuilder();
        for (int i = 0; i < source.getFieldCount(); i++) {
            // use sink field name
            if (source.getField(i).getType() == sink.getField(i).getType()) {
                final Field f = sink.getField(i);
                builder.addField(f.getName(), f.getType());
            } else {
                builder.addField(sink.getField(i).getName(), BasicType.STRING);
            }
        }
        return builder.build();
    }
}
