package io.logz.sawmill;

import io.logz.sawmill.annotations.Process;
import io.logz.sawmill.exceptions.ProcessorMissingException;
import io.logz.sawmill.processors.TestProcessor;
import org.reflections.Reflections;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Exchanger;

public class ProcessorFactories {
    private final Map<String, Processor.Factory> processorFactories;

    public ProcessorFactories() {
        this.processorFactories = new HashMap<>();
        loadProcessors();
    }

    private void loadProcessors() {
        Reflections reflections = new Reflections("io.logz.sawmill");
        Set<Class<?>> processorTypes =  reflections.getTypesAnnotatedWith(Process.class);
        processorTypes.forEach(type -> {
            try {
                String typeName = type.getAnnotation(Process.class).type();
                Optional<? extends Class<?>> factory = Arrays.stream(type.getDeclaredClasses()).filter(innerClasse -> innerClasse.getName().endsWith("$Factory")).findFirst();
                if (factory.isPresent()) {
                    processorFactories.put(typeName, (Processor.Factory) factory.get().getConstructor().newInstance());
                }
                else {
                    // skip processor
                }
            } catch (Exception e) {
                // skip processor
            }
        });

    }


    public Processor.Factory get(String type) {
        Processor.Factory factory = processorFactories.get(type);
        if (factory == null) throw new ProcessorMissingException("failed to parse processor type " + type);
        return factory;
    }
}
