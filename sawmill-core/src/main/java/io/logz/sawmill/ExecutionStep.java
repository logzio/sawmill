package io.logz.sawmill;

import java.util.List;

public class ExecutionStep {
    private final String name;
    private final Processor processor;
    private final List<Processor> onFailureProcessors;

    public ExecutionStep(String name, Processor processor, List<Processor> onFailureProcessors) {
        this.name = name;
        this.processor = processor;
        this.onFailureProcessors = onFailureProcessors;
    }

    public String getName() {
        return name;
    }

    public Processor getProcessor() {
        return processor;
    }

    public List<Processor> getOnFailureProcessors() {
        return onFailureProcessors;
    }
}
