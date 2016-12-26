package io.logz.sawmill.parser;

import java.util.List;
import java.util.Optional;

/**
 * Created by naorguetta on 20/12/2016.
 */
public class PipelineDefinition {
    private String name;
    private String description;
    private List<ExecutionStepDefinition> executionStepDefinitionList;
    private Optional<Boolean> isIgnoreFailure;

    public PipelineDefinition(String name, String description, List<ExecutionStepDefinition> executionStepDefinitionList, Boolean isIgnoreFailure) {
        this.name = name;
        this.description = description;
        this.executionStepDefinitionList = executionStepDefinitionList;
        this.isIgnoreFailure = Optional.ofNullable(isIgnoreFailure);
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

    public Optional<Boolean> isIgnoreFailure() {
        return isIgnoreFailure;
    }
}
