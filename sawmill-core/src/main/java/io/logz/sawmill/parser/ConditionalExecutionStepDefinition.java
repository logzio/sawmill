package io.logz.sawmill.parser;

import java.util.List;

/**
 * Created by naorguetta on 21/12/2016.
 */
public class ConditionalExecutionStepDefinition implements ExecutionStepDefinition {
    private ConditionDefinition condition;
    private List<ExecutionStepDefinition> onTrue;
    private List<ExecutionStepDefinition> onFalse;

    public ConditionalExecutionStepDefinition(ConditionDefinition condition, List<ExecutionStepDefinition> onTrue, List<ExecutionStepDefinition> onFalse) {
        this.condition = condition;
        this.onTrue = onTrue;
        this.onFalse = onFalse;
    }

    public ConditionDefinition getCondition() {
        return condition;
    }

    public List<ExecutionStepDefinition> getOnTrue() {
        return onTrue;
    }

    public List<ExecutionStepDefinition> getOnFalse() {
        return onFalse;
    }
}
