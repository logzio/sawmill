package io.logz.sawmill.parser;

import java.util.List;
import java.util.Optional;

public class PipelineDefinition {
    private List<ExecutionStepDefinition> executionStepDefinitionList;
    private Optional<Boolean> isIgnoreFailure;

    public PipelineDefinition(List<ExecutionStepDefinition> executionStepDefinitionList, Boolean isIgnoreFailure) {
        this.executionStepDefinitionList = executionStepDefinitionList;
        this.isIgnoreFailure = Optional.ofNullable(isIgnoreFailure);
    }

    public List<ExecutionStepDefinition> getExecutionSteps() {
        return executionStepDefinitionList;
    }

    public Optional<Boolean> isIgnoreFailure() {
        return isIgnoreFailure;
    }
}
