package io.logz.sawmill;

import java.util.List;
import java.util.Optional;

public class ExecutionStep {
    private final String processorName;
    private final Processor processor;
    private final Optional<List<Processor>> onFailureProcessors;

    public ExecutionStep(String processorName, Processor processor) {
        this(processorName, processor, null);
    }

    public ExecutionStep(String processorName, Processor processor, List<Processor> onFailureProcessors) {
        this.processorName = processorName;
        this.processor = processor;
        this.onFailureProcessors = Optional.ofNullable(onFailureProcessors);
    }

    public String getProcessorName() {
        return processorName;
    }

    public Processor getProcessor() {
        return processor;
    }

    public Optional<List<Processor>> getOnFailureProcessors() {
        return onFailureProcessors;
    }
}
