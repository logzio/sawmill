package io.logz.sawmill;

public class OnFailureExecutionStep {
    private final String processorName;
    private final Processor processor;

    public OnFailureExecutionStep(String processorName, Processor processor) {
        this.processorName = processorName;
        this.processor = processor;
    }

    public String getProcessorName() {
        return processorName;
    }

    public Processor getProcessor() {
        return processor;
    }
}
