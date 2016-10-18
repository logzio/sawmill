package io.logz.sawmill;

import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorMissingException;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

public class ProcessorFactories {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorFactories.class);
    private final Map<String, Processor.Factory> processorFactories;

    public ProcessorFactories() {
        this.processorFactories = new HashMap<>();
        loadProcessors();
    }

    private void loadProcessors() {
        Reflections reflections = new Reflections("io.logz.sawmill");
        Set<Class<?>> processorTypes =  reflections.getTypesAnnotatedWith(ProcessorProvider.class);
        processorTypes.forEach(type -> {
            try {
                String typeName = type.getAnnotation(ProcessorProvider.class).type();
                processorFactories.put(typeName, (Processor.Factory) type.getConstructor().newInstance());
            } catch (Exception e) {
                logger.error("failed to load processor, type={}", type.getName(), e);
            }
        });

    }


    public Processor.Factory get(String type) {
        Processor.Factory factory = processorFactories.get(type);
        if (factory == null) throw new ProcessorMissingException("failed to parse processor type " + type);
        return factory;
    }
}
