package io.logz.sawmill.parser;

import java.util.List;
import java.util.Optional;

public class ProcessorExecutionStepDefinition implements ExecutionStepDefinition {
    private ProcessorDefinition processorDefinition;
    private Optional<String> name;
    private Optional<List<ExecutionStepDefinition>> onFailureExecutionStepDefinitionList;
    private Optional<List<ExecutionStepDefinition>> onSuccessExecutionStepDefinitionList;

    public ProcessorExecutionStepDefinition(
            ProcessorDefinition processorDefinition,
            String name,
            List<ExecutionStepDefinition> onFailureExecutionStepDefinitionList,
            List<ExecutionStepDefinition> onSuccessExecutionStepDefinitionList) {
        this.processorDefinition = processorDefinition;
        this.name = Optional.ofNullable(name);
        this.onFailureExecutionStepDefinitionList = Optional.ofNullable(onFailureExecutionStepDefinitionList);
        this.onSuccessExecutionStepDefinitionList = Optional.ofNullable(onSuccessExecutionStepDefinitionList);
    }

    public Optional<String> getName() {
        return name;
    }

    public ProcessorDefinition getProcessorDefinition() {
        return processorDefinition;
    }

    public Optional<List<ExecutionStepDefinition>> getOnFailureExecutionStepDefinitionList() {
        return onFailureExecutionStepDefinitionList;
    }

    public Optional<List<ExecutionStepDefinition>> getOnSuccessExecutionStepDefinitionList() {
        return onSuccessExecutionStepDefinitionList;
    }
}
