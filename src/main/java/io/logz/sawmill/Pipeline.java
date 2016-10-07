package io.logz.sawmill;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Pipeline {

    private final String id;
    private final String description;
    private final Double version;
    private final List<Processor> processors;

    public Pipeline(String id, String description, Double version, List<Processor> processors) {
        if (id.isEmpty()) throw new IllegalArgumentException("id cannot be empty");
        if (CollectionUtils.isEmpty(processors)) throw new IllegalArgumentException("processors cannot be empty");
        this.id = id;
        this.description = description;
        this.version = version;
        this.processors = processors;
    }

    public String getId() { return id; }

    public String getDescription() { return description; }

    public Double getVersion() { return version; }

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

        public Pipeline create(Map<String, Object> mapConfig) {
            List<Processor> processors = new ArrayList<>();
            Gson gson = new Gson();
            JsonElement pipelineJson = gson.toJsonTree(mapConfig);
            Configuration pipelineConfiguration = gson.fromJson(pipelineJson, Configuration.class);

            pipelineConfiguration.getProcessors().entrySet().forEach(processor -> {
                Processor.Factory factory = processorFactories.get(processor.getKey());
                processors.add(factory.create(processor.getValue()));
            });

            return new Pipeline(pipelineConfiguration.getId(),
                    pipelineConfiguration.getDescription(),
                    pipelineConfiguration.getVersion(),
                    processors);
        }
    }

    public class Configuration {
        private String id;
        private String description;
        private Double version;
        private Map<String, Map> processors;

        public Configuration() { }

        public Configuration(String id, String description, Double version, Map<String,Map> processors) {
            this.id = id;
            this.description = description;
            this.version = version;
            this.processors = processors;
        }

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

        public Double getVersion() {
            return version;
        }

        public void setVersion(Double version) {
            this.version = version;
        }

        public Map<String,Map> getProcessors() {
            return processors;
        }

        public void setProcessors(Map<String,Map> processors) {
            this.processors = processors;
        }
    }
}
