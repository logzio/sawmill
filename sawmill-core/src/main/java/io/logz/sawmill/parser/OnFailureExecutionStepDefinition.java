package io.logz.sawmill.parser;

public class OnFailureExecutionStepDefinition {
    private ProcessorDefinition processorDefinition;
    private String name;

    public OnFailureExecutionStepDefinition(ProcessorDefinition processorDefinition, String name) {
        this.processorDefinition = processorDefinition;
        this.name = name;
    }

    public String getName() {
        return name;
    }

    public ProcessorDefinition getProcessorDefinition() {
        return processorDefinition;
    }
}
