package me.ericfu.lightning.conf;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import com.google.common.collect.ImmutableMap;
import me.ericfu.lightning.exception.InvalidConfigException;
import org.reflections.Reflections;

import java.io.File;
import java.util.Map;
import java.util.Set;

public class ConfReader {

    private final ObjectMapper mapper = new ObjectMapper(new YAMLFactory());

    private static final String PACKAGE = "me.ericfu.lightning";

    private static final String GENERAL = "general";
    private static final String SOURCE = "source";
    private static final String SINK = "sink";
    private static final String KIND = "kind";
    private static final String SPEC = "spec";

    private final File confFile;

    private GeneralConf generalConf;
    private SourceConf sourceConf;
    private SinkConf sinkConf;

    public ConfReader(File confFile) {
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

    private void readGeneralConf(JsonNode root) throws InvalidConfigException, JsonProcessingException {
        JsonNode general = root.get(GENERAL);
        if (general != null) {
            generalConf = mapper.treeToValue(general, GeneralConf.class);
        } else {
            // Just use default general conf
            generalConf = new GeneralConf();
        }
        generalConf.validate();
    }

    private void readSourceConf(JsonNode root) throws InvalidConfigException, JsonProcessingException {
        final JsonNode node = root.get(SOURCE);
        if (node == null) {
            throw new InvalidConfigException("data source config not found");
        } else if (node.get(KIND) == null) {
            throw new InvalidConfigException("kind of data source not specified");
        } else if (node.get(SPEC) == null) {
            throw new InvalidConfigException("spec of data source not specified");
        }

        final String kind = node.get(KIND).asText();
        Map<String, Class<? extends SourceConf>> sources = loadPlugins(SourceConf.class);
        Class<? extends SourceConf> clazz = sources.get(kind);
        if (clazz == null) {
            String kinds = String.join(", ", sources.keySet());
            throw new InvalidConfigException("unknown source kind: " + kind + ". available source kinds: " + kinds);
        }

        this.sourceConf = mapper.treeToValue(node.get(SPEC), clazz);
        this.sourceConf.validate();
    }

    private void readSinkConf(JsonNode root) throws InvalidConfigException, JsonProcessingException {
        final JsonNode node = root.get(SINK);
        if (node == null) {
            throw new InvalidConfigException("data sink config not found");
        } else if (node.get(KIND) == null) {
            throw new InvalidConfigException("kind of data sink not specified");
        } else if (node.get(SPEC) == null) {
            throw new InvalidConfigException("spec of data sink not specified");
        }

        final String kind = node.get(KIND).asText();
        Map<String, Class<? extends SinkConf>> sinks = loadPlugins(SinkConf.class);
        Class<? extends SinkConf> clazz = sinks.get(kind);
        if (clazz == null) {
            String kinds = String.join(", ", sinks.keySet());
            throw new InvalidConfigException("unknown sink kind: " + kind + ", available sink kinds: " + kinds);
        }

        this.sinkConf = mapper.treeToValue(node.get(SPEC), clazz);
        this.sinkConf.validate();
    }

    public GeneralConf getGeneralConf() {
        return generalConf;
    }

    public SourceConf getSourceConf() {
        return sourceConf;
    }

    public SinkConf getSinkConf() {
        return sinkConf;
    }

    /**
     * Get all plugins (source or sink) of given class
     *
     * @return a map from kind to the Source/Sink class
     */
    private <T extends Conf> Map<String, Class<? extends T>> loadPlugins(Class<T> clazz) {
        Reflections reflections = new Reflections(PACKAGE);
        Set<Class<? extends T>> subclasses = reflections.getSubTypesOf(clazz);

        ImmutableMap.Builder<String, Class<? extends T>> map = ImmutableMap.builder();
        for (Class<? extends T> c : subclasses) {
            String kind = c.getAnnotation(Kind.class).value();
            map.put(kind, c);
        }
        return map.build();
    }
}
