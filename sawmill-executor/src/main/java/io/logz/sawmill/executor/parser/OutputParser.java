package io.logz.sawmill.executor.parser;

import io.logz.sawmill.executor.Output;
import io.logz.sawmill.executor.OutputFactoryRegistry;

public class OutputParser {

    private OutputFactoryRegistry outputFactoryRegistry;

    public OutputParser(OutputFactoryRegistry outputFactoryRegistry) {
        this.outputFactoryRegistry = outputFactoryRegistry;
    }

    public Output parse(OutputDefinition outputDefinition) {
        Output.Factory factory = outputFactoryRegistry.get(outputDefinition.getType());
        return factory.create(outputDefinition.getConfig());
    }
}
