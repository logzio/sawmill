package io.logz.sawmill.parser;

import java.util.List;
import java.util.Optional;

public class ProcessorExecutionStepDefinition implements ExecutionStepDefinition {
    private ProcessorDefinition processorDefinition;
    private String name;
    private Optional<List<ExecutionStepDefinition>> onFailureExecutionStepDefinitionList;

    public ProcessorExecutionStepDefinition(
            ProcessorDefinition processorDefinition,
            String name,
            List<ExecutionStepDefinition> onFailureExecutionStepDefinitionList) {
        this.processorDefinition = processorDefinition;
        this.name = name;
        this.onFailureExecutionStepDefinitionList = Optional.ofNullable(onFailureExecutionStepDefinitionList);
    }

    public String getName() {
        return name;
    }

    public ProcessorDefinition getProcessorDefinition() {
        return processorDefinition;
    }

    public Optional<List<ExecutionStepDefinition>> getOnFailureExecutionStepDefinitionList() {
        return onFailureExecutionStepDefinitionList;
    }
}
