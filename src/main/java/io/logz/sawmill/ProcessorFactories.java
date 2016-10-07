package io.logz.sawmill;

import io.logz.sawmill.exceptions.ProcessorMissingException;
import io.logz.sawmill.processors.TestProcessor;

import java.util.HashMap;
import java.util.Map;

public class ProcessorFactories {
    private final Map<String, Processor.Factory> processorFactories;

    public ProcessorFactories() {
        this.processorFactories = new HashMap<>();

        loadBaseProcessors();
        loadPluginProcessors();
    }

    private void loadPluginProcessors() {
    }

    private void loadBaseProcessors() {
        processorFactories.put(TestProcessor.TYPE, new TestProcessor.Factory());
    }

    public Processor.Factory get(String type) {
        Processor.Factory factory = processorFactories.get(type);
        if (factory == null) throw new ProcessorMissingException("failed to parse processor type " + type);
        return factory;
    }
}
