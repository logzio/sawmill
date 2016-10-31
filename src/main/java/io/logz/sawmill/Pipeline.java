package io.logz.sawmill;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;

public class Pipeline {

    private final String id;
    private final String description;
    private final List<Processor> processors;

    public Pipeline(String id, String description, List<Processor> processors) {
        checkState(!id.isEmpty(), "id cannot be empty");
        checkState(!CollectionUtils.isEmpty(processors), "processors cannot be empty");
        this.id = id;
        this.description = description;
        this.processors = processors;
    }

    public String getId() { return id; }

    public String getDescription() { return description; }

    public List<Processor> getProcessors() { return processors; }

    public static final class Factory {

        private final ProcessorFactoryRegistry processorFactoryRegistry;

        public Factory() {
            this.processorFactoryRegistry = new ProcessorFactoryRegistry();
        }

        public Factory(ProcessorFactoryRegistry processorFactoryRegistry) {
            this.processorFactoryRegistry = processorFactoryRegistry;
        }

        public Pipeline create(Configuration config) {
            List<Processor> processors = new ArrayList<>();

            config.getProcessors().forEach(processorDefinition -> {
                Processor.Factory factory = processorFactoryRegistry.get(processorDefinition.getName());
                processors.add(factory.create(JsonUtils.toJsonString(processorDefinition.getConfig())));
            });

            return new Pipeline(config.getId(),
                    config.getDescription(),
                    processors);
        }

        public Pipeline create(String config, ConfigurationType type) {
            switch (type) {
                case JSON:
                    return create(JsonUtils.fromJsonString(Pipeline.Configuration.class, config));
                case HOCON:
                    String configJson = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
                    return create(JsonUtils.fromJsonString(Pipeline.Configuration.class, configJson));
                default:
                    throw new IllegalArgumentException("undefined configuration type");
            }
        }
    }

    public enum ConfigurationType {
        JSON,
        HOCON
    }

    public static class Configuration {
        private String id;
        private String description;
        private List<ProcessorDefinition> processors;

        public Configuration() { }

        public String getId() {
            return id;
        }

        public String getDescription() {
            return description;
        }

        public List<ProcessorDefinition> getProcessors() {
            return processors;
        }
    }

    public static class ProcessorDefinition {
        private String name;
        private Map<String,Object> config;

        public ProcessorDefinition() { }

        public String getName() {
            return name;
        }

        public Map<String,Object> getConfig() {
            return config;
        }
    }
}
