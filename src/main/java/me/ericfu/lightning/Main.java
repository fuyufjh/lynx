package me.ericfu.lightning;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import me.ericfu.lightning.exception.InvalidConfigException;
import me.ericfu.lightning.schema.RecordType;
import me.ericfu.lightning.sink.SchemaSink;
import me.ericfu.lightning.sink.SchemalessSink;
import me.ericfu.lightning.sink.Sink;
import me.ericfu.lightning.sink.SinkFactory;
import me.ericfu.lightning.source.SchemaSource;
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

        RecordType sourceSchema = source instanceof SchemaSource ? ((SchemaSource) source).getSchema() : null;
        RecordType sinkSchema = sink instanceof SchemaSink ? ((SchemaSink) sink).getSchema() : null;

        if (sourceSchema == null && sinkSchema == null) {
            logger.error("At least one of source/sink should be with schema");
            return;
        } else if (sourceSchema != null && sinkSchema != null) {
            logger.error("TODO: not implemented");
            return;
        } else if (sourceSchema != null) {
            assert sink instanceof SchemalessSink;
            ((SchemalessSink) sink).setSchema(sourceSchema);
        } else {
            assert source instanceof SchemalessSource;
            ((SchemalessSource) source).setSchema(sinkSchema);
        }

        ThreadPoolExecutor threadPool =
            new ThreadPoolExecutor(1, 32, 1, TimeUnit.SECONDS, new LinkedBlockingQueue<>(),
                new ThreadFactoryBuilder().setDaemon(true).setNameFormat("worker-%d").build());

        Future<?> future = threadPool.submit(new Pipeline(source, sink));

        try {
            future.get();
        } catch (InterruptedException | ExecutionException ex) {
            logger.error("transfer failed", ex);
            return;
        }

        threadPool.shutdown();
        logger.info("Bye!");
    }
}
