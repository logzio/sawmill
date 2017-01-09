package io.logz.sawmill;

import java.util.List;
import java.util.Optional;

public class ProcessorExecutionStep implements ExecutionStep {
    private final String processorName;
    private final Processor processor;
    private final Optional<List<ExecutionStep>> onFailureExecutionSteps;

    public ProcessorExecutionStep(String processorName, Processor processor) {
        this(processorName, processor, null);
    }

    public ProcessorExecutionStep(String processorName, Processor processor, List<ExecutionStep> onFailureExecutionSteps) {
        this.processorName = processorName;
        this.processor = processor;
        this.onFailureExecutionSteps = Optional.ofNullable(onFailureExecutionSteps);
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
}
