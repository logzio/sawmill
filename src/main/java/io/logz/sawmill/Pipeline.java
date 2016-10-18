package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public class Pipeline {

    private final String id;
    private final String description;
    private final List<Processor> processors;

    public Pipeline(String id, String description, List<Processor> processors) {
        if (id.isEmpty()) throw new IllegalArgumentException("id cannot be empty");
        if (CollectionUtils.isEmpty(processors)) throw new IllegalArgumentException("processors cannot be empty");
        this.id = id;
        this.description = description;
        this.processors = processors;
    }

    public String getId() { return id; }

    public String getDescription() { return description; }

    public List<Processor> getProcessors() { return processors; }

    public void execute(Log log) throws PipelineExecutionException {
        for (Processor processor : processors) {
            try {
                processor.execute(log);
            } catch (Exception e) {
                throw new PipelineExecutionException(String.format("failed to execute processor %s on log %s", processor.getType(), log.toString()), e);
            }
        }
    }

    public static final class Factory {

        private final ProcessorFactories processorFactories = new ProcessorFactories();

        public Pipeline create(Configuration config) {
            List<Processor> processors = new ArrayList<>();

            config.getProcessors().forEach(processorDefinition -> {
                Processor.Factory factory = processorFactories.get(processorDefinition.getName());
                processors.add(factory.create(JsonUtils.toJsonString(processorDefinition.getConfig())));
            });

            return new Pipeline(config.getId(),
                    config.getDescription(),
                    processors);
        }
    }

    public static class Configuration {
        private String id;
        private String description;
        private List<ProcessorDefinition> processors;

        public Configuration() { }

        public String getId() {
            return id;
        }

        public void setId(String id) {
            this.id = id;
        }

        public String getDescription() {
            return description;
        }

        public void setDescription(String description) {
            this.description = description;
        }

        public List<ProcessorDefinition> getProcessors() {
            return processors;
        }

        public void setProcessors(List<ProcessorDefinition> processors) {
            this.processors = processors;
        }
    }

    public static class ProcessorDefinition {
        private String name;
        private Object config;

        public ProcessorDefinition() { }

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Object getConfig() {
            return config;
        }

        public void setConfig(Object config) {
            this.config = config;
        }
    }
}
