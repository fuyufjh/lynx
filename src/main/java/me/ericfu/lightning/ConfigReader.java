package me.ericfu.lightning;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import me.ericfu.lightning.conf.GeneralConf;
import me.ericfu.lightning.conf.SinkConf;
import me.ericfu.lightning.conf.SourceConf;
import me.ericfu.lightning.exception.InvalidConfigException;
import me.ericfu.lightning.sink.jdbc.JdbcSinkConf;
import me.ericfu.lightning.source.random.RandomSourceConf;
import me.ericfu.lightning.source.text.TextSourceConf;

import java.io.File;

public class ConfigReader {

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private final File confFile;

    private GeneralConf generalConf;
    private SourceConf sourceConf;
    private SinkConf sinkSpec;

    public ConfigReader(File confFile) {
        this.confFile = confFile;
    }

    public void readConfig() throws InvalidConfigException {
        try {
            JsonNode root = mapper.readValue(confFile, JsonNode.class);

            readGeneralConf(root);
            readSourceConf(root);
            readSinkConf(root);
        } catch (InvalidConfigException ex) {
            throw ex;
        } catch (Exception ex) {
            throw new InvalidConfigException(ex.getMessage());
        }
    }

    private void readGeneralConf(JsonNode root) throws JsonProcessingException {
        JsonNode general = root.get("general");
        if (general != null) {
            generalConf = mapper.treeToValue(general, GeneralConf.class);
        } else {
            // Just use default general conf
            generalConf = new GeneralConf();
        }
    }

    private void readSourceConf(JsonNode root) throws InvalidConfigException, JsonProcessingException {
        final JsonNode node = root.get("source");
        if (node == null) {
            throw new InvalidConfigException("data source config not found");
        }
        if (node.get("kind") == null) {
            throw new InvalidConfigException("kind of data source not specified");
        }
        if (node.get("spec") == null) {
            throw new InvalidConfigException("spec of data source not specified");
        }

        String sourceKind = node.get("kind").asText();
        switch (sourceKind.toLowerCase()) {
        case "text":
            sourceConf = mapper.treeToValue(node.get("spec"), TextSourceConf.class);
            break;
        case "random":
            sourceConf = mapper.treeToValue(node.get("spec"), RandomSourceConf.class);
            break;
        default:
            throw new InvalidConfigException("data source '" + sourceKind + "' not supported");
        }

        sourceConf.validate();
    }

    private void readSinkConf(JsonNode root) throws InvalidConfigException, JsonProcessingException {
        final JsonNode node = root.get("sink");
        if (node == null) {
            throw new InvalidConfigException("data sink config not found");
        }
        if (node.get("kind") == null) {
            throw new InvalidConfigException("kind of data sink not specified");
        }
        if (node.get("spec") == null) {
            throw new InvalidConfigException("spec of data sink not specified");
        }

        String sinkKind = node.get("kind").asText();
        switch (sinkKind.toLowerCase()) {
        case "jdbc":
            sinkSpec = mapper.treeToValue(node.get("spec"), JdbcSinkConf.class);
            break;
        default:
            throw new InvalidConfigException("data sink '" + sinkKind + "' not supported");
        }

        sinkSpec.validate();
    }

    public GeneralConf getGeneralConf() {
        return generalConf;
    }

    public SourceConf getSourceConf() {
        return sourceConf;
    }

    public SinkConf getSinkConf() {
        return sinkSpec;
    }

}
