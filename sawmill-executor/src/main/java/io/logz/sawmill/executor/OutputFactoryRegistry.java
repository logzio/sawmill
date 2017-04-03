package io.logz.sawmill.executor;

import io.logz.sawmill.executor.exceptions.OutputMissingException;

import java.util.HashMap;
import java.util.Map;

public class OutputFactoryRegistry {
    private final Map<String, Output.Factory> outputFactories = new HashMap<>();

    public OutputFactoryRegistry() { }

    public void register(String name, Output.Factory factory) {
        outputFactories.put(name, factory);
    }

    public Output.Factory get(String name) {
        Output.Factory factory = outputFactories.get(name);
        if (factory == null) throw new OutputMissingException("No output registered with name " + name);
        return factory;
    }
}
