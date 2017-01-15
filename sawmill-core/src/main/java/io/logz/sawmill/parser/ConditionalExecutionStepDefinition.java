package io.logz.sawmill.parser;

import java.util.List;
import java.util.Optional;

public class ConditionalExecutionStepDefinition implements ExecutionStepDefinition {
    private ConditionDefinition condition;
    private List<ExecutionStepDefinition> onTrue;
    private Optional<List<ExecutionStepDefinition>> onFalse;

    public ConditionalExecutionStepDefinition(ConditionDefinition condition, List<ExecutionStepDefinition> onTrue, List<ExecutionStepDefinition> onFalse) {
        this.condition = condition;
        this.onTrue = onTrue;
        this.onFalse = Optional.ofNullable(onFalse);
    }

    public ConditionDefinition getConditionDefinition() {
        return condition;
    }

    public List<ExecutionStepDefinition> getOnTrue() {
        return onTrue;
    }

    public Optional<List<ExecutionStepDefinition>> getOnFalse() {
        return onFalse;
    }
}
