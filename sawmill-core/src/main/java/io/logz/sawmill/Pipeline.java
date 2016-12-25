package io.logz.sawmill;

import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

import static com.google.common.base.Preconditions.checkState;

public class Pipeline {

    private final String id;
    private final String name;
    private final String description;
    private final List<ExecutionStep> executionSteps;
    private final boolean ignoreFailure;

    public Pipeline(String id, String name, String description, List<ExecutionStep> executionSteps, boolean ignoreFailure) {
        checkState(!id.isEmpty(), "id cannot be empty");
        checkState(CollectionUtils.isNotEmpty(executionSteps), "executionSteps cannot be empty");

        this.id = id;
        this.name = name;
        this.description = description;
        this.executionSteps = executionSteps;
        this.ignoreFailure = ignoreFailure;
    }

    public String getId() { return id; }

    public String getName() { return name; }

    public String getDescription() { return description; }

    public List<ExecutionStep> getExecutionSteps() { return executionSteps; }

    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    public static final class Factory {

        private final ProcessorFactoryRegistry processorFactoryRegistry;

        public Factory() {
            this.processorFactoryRegistry = new ProcessorFactoryRegistry();
            ProcessorFactoriesLoader.getInstance().loadAnnotatedProcessors(processorFactoryRegistry);
        }

        public Factory(ProcessorFactoryRegistry processorFactoryRegistry) {
            this.processorFactoryRegistry = processorFactoryRegistry;
        }

        public Pipeline create(String config) {
            return create(UUID.randomUUID().toString(), config);
        }

        public Pipeline create(String id, String config) {
            String configJson = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
            return create(id, JsonUtils.fromJsonString(Definition.class, configJson));
        }

        private Pipeline create(String id, Definition config) {
            List<ExecutionStep> executionSteps = config.getProcessors().stream()
                    .map(this::extractExecutionStep)
                    .collect(Collectors.toList());

            return new Pipeline(id,
                    config.getName(),
                    config.getDescription(),
                    executionSteps,
                    config.getIgnoreFailure());
        }

        private ExecutionStep extractExecutionStep(ProcessorDefinition processorDefinition) {
            Processor processor = extractProcessor(processorDefinition);

            List<OnFailureExecutionStep> onFailureProcessors = null;
            if (CollectionUtils.isNotEmpty(processorDefinition.getOnFailure())) {
                onFailureProcessors = processorDefinition.getOnFailure().stream()
                        .map(this::extractOnFailureExecutionStep)
                        .collect(Collectors.toList());
            }

            return new ExecutionStep(processorDefinition.getName(),
                    processor,
                    onFailureProcessors);
        }

        private OnFailureExecutionStep extractOnFailureExecutionStep(ProcessorDefinition processorDefinition) {
            return new OnFailureExecutionStep(processorDefinition.getName(), extractProcessor(processorDefinition));
        }

        private Processor extractProcessor(ProcessorDefinition definition) {
            Processor.Factory factory = processorFactoryRegistry.get(definition.getType());
            return factory.create(definition.getConfig());
        }
    }

    public static class Definition {
        private String name;
        private String description;
        private List<ProcessorDefinition> processors;
        private boolean ignoreFailure = true;

        public Definition() { }

        public String getName() { return name; }

        public String getDescription() {
            return description;
        }

        public List<ProcessorDefinition> getProcessors() {
            return processors;
        }

        public boolean getIgnoreFailure() {
            return ignoreFailure;
        }
    }
}
