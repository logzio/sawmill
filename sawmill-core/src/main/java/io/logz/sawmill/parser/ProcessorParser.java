package io.logz.sawmill.parser;

import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorFactoryRegistry;

/**
 * Created by naorguetta on 04/01/2017.
 */
public class ProcessorParser {
    private final ProcessorFactoryRegistry processorFactoryRegistry;

    public ProcessorParser(ProcessorFactoryRegistry processorFactoryRegistry) {
        this.processorFactoryRegistry = processorFactoryRegistry;
    }

    public Processor parse(ProcessorDefinition processorDefinition) {
        Processor.Factory factory = processorFactoryRegistry.get(processorDefinition.getType());
        return factory.create(processorDefinition.getConfig());
    }
}
