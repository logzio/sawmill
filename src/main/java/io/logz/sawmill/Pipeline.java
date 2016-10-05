package io.logz.sawmill;

import io.logz.sawmill.exceptions.PipelineExecutionException;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Map;

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

        private final ProcessorFactories processorFactories;

        public Factory(ProcessorFactories processorFactories) {
            this.processorFactories = processorFactories;
        }

        public Pipeline create(Map<String, Object> config) {
            return null;
        }
    }
}
