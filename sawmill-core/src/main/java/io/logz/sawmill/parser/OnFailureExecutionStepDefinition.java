package io.logz.sawmill.parser;

/**
 * Created by naorguetta on 21/12/2016.
 */
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
