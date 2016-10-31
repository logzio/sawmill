package io.logz.sawmill;

import io.logz.sawmill.exceptions.ProcessorMissingException;

import java.util.HashMap;
import java.util.Map;

public class ProcessorFactoryRegistry {
    private final Map<String, Processor.Factory> processorFactories = new HashMap<>();

    public ProcessorFactoryRegistry() { }

    public void register(String name, Processor.Factory factory) {
        processorFactories.put(name, factory);
    }

    public Processor.Factory get(String name) {
        Processor.Factory factory = processorFactories.get(name);
        if (factory == null) throw new ProcessorMissingException("No such processor with name " + name);
        return factory;
    }
}
