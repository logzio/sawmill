package io.logz.sawmill;

import java.util.List;

public class ConditionalExecutionStep implements ExecutionStep {

    private Condition condition;
    private List<ExecutionStep> onTrue;
    private List<ExecutionStep> onFalse;

    public ConditionalExecutionStep(Condition condition, List<ExecutionStep> onTrue, List<ExecutionStep> onFalse) {
        this.condition = condition;
        this.onTrue = onTrue;
        this.onFalse = onFalse;
    }

    public Condition getCondition() {
        return condition;
    }

    public List<ExecutionStep> getOnTrue() {
        return onTrue;
    }

    public List<ExecutionStep> getOnFalse() {
        return onFalse;
    }
}
