package io.logz.sawmill.parser;

import java.util.List;
import java.util.Optional;

public class ProcessorExecutionStepDefinition implements ExecutionStepDefinition {
    private ProcessorDefinition processorDefinition;
    private String name;
    private Optional<List<OnFailureExecutionStepDefinition>> onFailureExecutionStepDefinitionList;

    public ProcessorExecutionStepDefinition(
            ProcessorDefinition processorDefinition,
            String name,
            List<OnFailureExecutionStepDefinition> onFailureExecutionStepDefinitionList) {
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

    public Optional<List<OnFailureExecutionStepDefinition>> getOnFailureExecutionStepDefinitionList() {
        return onFailureExecutionStepDefinitionList;
    }
}
