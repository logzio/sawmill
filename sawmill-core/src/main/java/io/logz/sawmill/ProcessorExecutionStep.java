package io.logz.sawmill;

import java.util.List;
import java.util.Optional;

public class ProcessorExecutionStep implements ExecutionStep {
    private final String processorName;
    private final Processor processor;
    private final Optional<List<ExecutionStep>> onFailureExecutionSteps;
    private final Optional<List<ExecutionStep>> onSuccessExecutionSteps;

    public ProcessorExecutionStep(String processorName, Processor processor) {
        this(processorName, processor, null);
    }

    public ProcessorExecutionStep(String processorName, Processor processor, List<ExecutionStep> onFailureExecutionSteps) {
        this(processorName, processor, onFailureExecutionSteps, null);
    }

    public ProcessorExecutionStep(String processorName, Processor processor, List<ExecutionStep> onFailureExecutionSteps,
                                  List<ExecutionStep> onSuccessExecutionSteps) {
        this.processorName = processorName;
        this.processor = processor;
        this.onFailureExecutionSteps = Optional.ofNullable(onFailureExecutionSteps);
        this.onSuccessExecutionSteps = Optional.ofNullable(onSuccessExecutionSteps);
    }

    public String getProcessorName() {
        return processorName;
    }

    public Processor getProcessor() {
        return processor;
    }

    public Optional<List<ExecutionStep>> getOnFailureExecutionSteps() {
        return onFailureExecutionSteps;
    }

    public Optional<List<ExecutionStep>> getOnSuccessExecutionSteps() {
        return onSuccessExecutionSteps;
    }
}
