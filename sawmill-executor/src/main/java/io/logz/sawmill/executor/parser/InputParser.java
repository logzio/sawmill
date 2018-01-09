package io.logz.sawmill.executor.parser;

import io.logz.sawmill.executor.Input;
import io.logz.sawmill.executor.InputFactoryRegistry;

public class InputParser {

    private InputFactoryRegistry inputFactoryRegistry;

    public InputParser(InputFactoryRegistry inputFactoryRegistry) {
        this.inputFactoryRegistry = inputFactoryRegistry;
    }

    public Input parse(InputDefinition inputDefinition) {
        Input.Factory factory = inputFactoryRegistry.get(inputDefinition.getType());
        return factory.create(inputDefinition.getConfig());
    }
}
