package io.logz.sawmill;

import io.logz.sawmill.parser.ExecutionStepsParser;
import io.logz.sawmill.parser.PipelineDefinition;
import io.logz.sawmill.parser.PipelineDefinitionParser;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;

public class Pipeline {

    private final String id;
    private final List<ExecutionStep> executionSteps;
    private final boolean ignoreFailure;

    public Pipeline(String id, List<ExecutionStep> executionSteps, boolean ignoreFailure) {
        checkState(!id.isEmpty(), "id cannot be empty");
        checkState(CollectionUtils.isNotEmpty(executionSteps), "executionSteps cannot be empty");

        this.id = id;
        this.executionSteps = executionSteps;
        this.ignoreFailure = ignoreFailure;
    }

    public String getId() { return id; }

    public List<ExecutionStep> getExecutionSteps() { return executionSteps; }

    public boolean isIgnoreFailure() {
        return ignoreFailure;
    }

    public static final class Factory {

        private final PipelineDefinitionParser pipelineDefinitionParser;
        private final ExecutionStepsParser executionStepsParser;

        public Factory() {
            this(new ProcessorFactoryRegistry(), new ConditionFactoryRegistry());
        }

        public Factory(ProcessorFactoryRegistry processorFactoryRegistry, ConditionFactoryRegistry conditionFactoryRegistry) {
            ProcessorFactoriesLoader.getInstance().loadAnnotatedProcessors(processorFactoryRegistry);
            ConditionalFactoriesLoader.getInstance().loadAnnotatedProcessors(conditionFactoryRegistry);
            pipelineDefinitionParser = new PipelineDefinitionParser();
            executionStepsParser = new ExecutionStepsParser(processorFactoryRegistry, conditionFactoryRegistry);
        }

        public Pipeline create(String config) {
            String id = UUID.randomUUID().toString();
            return create(id, config);
        }

        public Pipeline create(String id, String config) {
            PipelineDefinition pipelineDefinition = pipelineDefinitionParser.parse(config);
            return create(id, pipelineDefinition);
        }

        public Pipeline create(String id, PipelineDefinition pipelineDefinition) {
            List<ExecutionStep> executionSteps = executionStepsParser.parse(pipelineDefinition.getExecutionSteps());
            Optional<Boolean> ignoreFailureNullable = pipelineDefinition.isIgnoreFailure();
            boolean ignoreFailure = ignoreFailureNullable.orElse(true);

            return new Pipeline(id, executionSteps, ignoreFailure);
        }

    }
}
