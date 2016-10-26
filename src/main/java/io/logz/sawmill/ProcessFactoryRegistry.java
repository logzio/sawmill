package io.logz.sawmill;

import io.logz.sawmill.exceptions.ProcessorMissingException;

import java.util.HashMap;
import java.util.Map;

public class ProcessFactoryRegistry {
    private final Map<String, Process.Factory> processorFactories = new HashMap<>();

    public ProcessFactoryRegistry() { }

    public void register(String name, Process.Factory factory) {
        processorFactories.put(name, factory);
    }

    public Process.Factory get(String name) {
        Process.Factory factory = processorFactories.get(name);
        if (factory == null) throw new ProcessorMissingException("No such process with name " + name);
        return factory;
    }
}
