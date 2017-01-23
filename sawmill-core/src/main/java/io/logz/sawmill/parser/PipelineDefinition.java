package io.logz.sawmill.parser;

import java.util.List;
import java.util.Optional;

public class PipelineDefinition {
    private List<ExecutionStepDefinition> executionStepDefinitionList;
    private Optional<Boolean> stopOnFailure;

    public PipelineDefinition(List<ExecutionStepDefinition> executionStepDefinitionList, Boolean stopOnFailure) {
        this.executionStepDefinitionList = executionStepDefinitionList;
        this.stopOnFailure = Optional.ofNullable(stopOnFailure);
    }

    public List<ExecutionStepDefinition> getExecutionSteps() {
        return executionStepDefinitionList;
    }

    public Optional<Boolean> isStopOnFailure() {
        return stopOnFailure;
    }
}
