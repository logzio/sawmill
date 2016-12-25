package io.logz.sawmill.parser;

import io.logz.sawmill.parser.ExecutionStepDefinition;

import java.util.List;

/**
 * Created by naorguetta on 20/12/2016.
 */
public class PipelineDefinition {
    private String name;
    private String description;
    private List<ExecutionStepDefinition> executionStepDefinitionList;
    private Boolean isIgnoreFailure;

    public PipelineDefinition(String name, String description, List<ExecutionStepDefinition> executionStepDefinitionList, Boolean isIgnoreFailure) {
        this.name = name;
        this.description = description;
        this.executionStepDefinitionList = executionStepDefinitionList;
        this.isIgnoreFailure = isIgnoreFailure;
    }

    public String getName() {
        return name;
    }

    public String getDescription() {
        return description;
    }

    public List<ExecutionStepDefinition> getExecutionSteps() {
        return executionStepDefinitionList;
    }

    public Boolean isIgnoreFailure() {
        return isIgnoreFailure;
    }
}
