package io.logz.sawmill.executor;

import io.logz.sawmill.executor.exceptions.InputMissingException;

import java.util.HashMap;
import java.util.Map;

public class InputFactoryRegistry {
    private final Map<String, Input.Factory> inputFactories = new HashMap<>();

    public InputFactoryRegistry() { }

    public void register(String name, Input.Factory factory) {
        inputFactories.put(name, factory);
    }

    public Input.Factory get(String name) {
        Input.Factory factory = inputFactories.get(name);
        if (factory == null) throw new InputMissingException("No input registered with name " + name);
        return factory;
    }
}
