package io.logz.sawmill.parser;

import java.util.List;
import java.util.Map;

/**
 * Created by naorguetta on 21/12/2016.
 */
public class ProcessorExecutionStepDefinition implements ExecutionStepDefinition {
    private ProcessorDefinition processorDefinition;
    private String name;
    private List<OnFailureExecutionStepDefinition> onFailureExecutionStepDefinitionList;

    public ProcessorExecutionStepDefinition(ProcessorDefinition processorDefinition, String name, List<OnFailureExecutionStepDefinition> onFailureExecutionStepDefinitionList) {
        this.processorDefinition = processorDefinition;
        this.name = name;
        this.onFailureExecutionStepDefinitionList = onFailureExecutionStepDefinitionList;
    }

    public String getName() {
        return name;
    }

    public List<OnFailureExecutionStepDefinition> getOnFailureExecutionStepDefinitionList() {
        return onFailureExecutionStepDefinitionList;
    }

    public ProcessorDefinition getProcessorDefinition() {
        return processorDefinition;
    }
}
