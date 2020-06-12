package io.logz.sawmill;

import io.logz.sawmill.parser.ExecutionStepsParser;
import io.logz.sawmill.parser.PipelineDefinition;
import io.logz.sawmill.parser.PipelineDefinitionJsonParser;
import org.apache.commons.collections4.CollectionUtils;

import java.util.List;
import java.util.Optional;
import java.util.UUID;

import static com.google.common.base.Preconditions.checkState;

public class Pipeline {

    private final String id;
    private final List<ExecutionStep> executionSteps;
    private final boolean stopOnFailure;

    public Pipeline(String id, List<ExecutionStep> executionSteps, boolean stopOnFailure) {
        checkState(!id.isEmpty(), "id cannot be empty");
        checkState(CollectionUtils.isNotEmpty(executionSteps), "executionSteps cannot be empty");

        this.id = id;
        this.executionSteps = executionSteps;
        this.stopOnFailure = stopOnFailure;
    }

    public String getId() { return id; }

    public List<ExecutionStep> getExecutionSteps() { return executionSteps; }

    public boolean isStopOnFailure() {
        return stopOnFailure;
    }

    public static final class Factory {

        private final ExecutionStepsParser executionStepsParser;

        public Factory(SawmillConfiguration... sawmillConfigurations) {
            this(
                    new ProcessorFactoryRegistry(new ProcessorFactoriesLoader(new TemplateService(), sawmillConfigurations)),
                    new ConditionFactoryRegistry(new ConditionalFactoriesLoader(new TemplateService(), sawmillConfigurations))
            );
        }

        public Factory(ProcessorFactoryRegistry processorFactoryRegistry,
                       ConditionFactoryRegistry conditionFactoryRegistry) {

            executionStepsParser = new ExecutionStepsParser(processorFactoryRegistry, conditionFactoryRegistry);
        }

        public Pipeline create(String config) {
            String id = UUID.randomUUID().toString();
            return create(id, config);
        }

        public Pipeline create(String id, String config) {
            PipelineDefinition pipelineDefinition = PipelineDefinitionJsonParser.parse(config);
            return create(id, pipelineDefinition);
        }

        public Pipeline create(String id, PipelineDefinition pipelineDefinition) {
            List<ExecutionStep> executionSteps = executionStepsParser.parse(pipelineDefinition.getExecutionSteps());
            Optional<Boolean> stopOnFailureNullable = pipelineDefinition.isStopOnFailure();
            boolean stopOnFailure = stopOnFailureNullable.orElse(false);

            return new Pipeline(id, executionSteps, stopOnFailure);
        }
    }
}
