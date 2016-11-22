package io.logz.sawmill;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkState;
import static io.logz.sawmill.Pipeline.FailureHandler.ABORT;

public class Pipeline {

    private final String id;
    private final String name;
    private final String description;
    private final List<Processor> processors;
    private final FailureHandler failureHandler;

    public Pipeline(String id, String name, String description, List<Processor> processors, FailureHandler failureHandler) {
        checkState(!id.isEmpty(), "id cannot be empty");
        checkState(CollectionUtils.isNotEmpty(processors), "processors cannot be empty");

        this.id = id;
        this.name = name;
        this.description = description;
        this.processors = processors;
        this.failureHandler = failureHandler;
    }

    public String getId() { return id; }

    public String getName() { return name; }

    public String getDescription() { return description; }

    public List<Processor> getProcessors() { return processors; }

    public FailureHandler getFailureHandler() {
        return failureHandler;
    }

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
                processors.add(factory.create(JsonUtils.toJsonString(processorDefinition.getConfig()), processorFactoryRegistry));
            });

            return new Pipeline(config.getId(),
                    config.getName(),
                    config.getDescription(),
                    processors,
                    config.getFailureHandler());
        }

        public Pipeline create(String config) {
            String configJson = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
            return create(JsonUtils.fromJsonString(Pipeline.Configuration.class, configJson));
        }
    }

    public static class Configuration {
        private String id;
        private String name;
        private String description;
        private List<Processor.ProcessorDefinition> processors;
        private FailureHandler failureHandler;

        public Configuration() { }

        public String getId() {
            return id;
        }

        public String getName() { return name; }

        public String getDescription() {
            return description;
        }

        public List<Processor.ProcessorDefinition> getProcessors() {
            return processors;
        }

        public FailureHandler getFailureHandler() {
            return failureHandler != null ? failureHandler : ABORT;
        }
    }

    public enum FailureHandler {
        CONTINUE,
        DROP,
        ABORT;

        @Override
        public String toString() {
            return this.name().toLowerCase();
        }
    }
}
