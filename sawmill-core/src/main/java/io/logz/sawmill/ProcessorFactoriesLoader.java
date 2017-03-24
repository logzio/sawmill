package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.annotations.ProcessorProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ProcessorFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorFactoriesLoader.class);
    private static ProcessorFactoriesLoader instance;
    private final Reflections reflections;
    private final Map<Class<? extends Service>, Service> services;

    private ProcessorFactoriesLoader() {
        this(new TemplateService());
    }

    public ProcessorFactoriesLoader(TemplateService templateService) {
        reflections = new Reflections("io.logz.sawmill");
        services = new HashMap<>();
        services.put(TemplateService.class, templateService);
    }

    public static ProcessorFactoriesLoader getInstance() {
        if (instance == null) {
            instance = new ProcessorFactoriesLoader();
        }

        return instance;
    }

    public void loadAnnotatedProcessors(ProcessorFactoryRegistry processorFactoryRegistry) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        int processorsLoaded = 0;
        Set<Class<?>> processors = reflections.getTypesAnnotatedWith(ProcessorProvider.class);
        for (Class<?> processor : processors) {
            try {
                ProcessorProvider annotation = processor.getAnnotation(ProcessorProvider.class);
                String typeName = annotation.type();
                processorFactoryRegistry.register(typeName, getFactory(annotation));
                logger.info("{} processor factory loaded successfully, took {}ms", typeName, stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                processorsLoaded++;
            } catch (Exception e) {
                logger.error("failed to load processor {}", processor.getName(), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(MILLISECONDS);
            }
        }
        logger.info("{} processor factories loaded, took {}ms", processorsLoaded, stopwatch.elapsed(MILLISECONDS));
    }

    public Processor.Factory getFactory(ProcessorProvider annotation) throws InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, NoSuchMethodException {
        Class<? extends Processor.Factory> factoryType = annotation.factory();
        Class<? extends Service>[] servicesToInject = annotation.services();
        Object[] servicesInstance = Stream.of(servicesToInject).map(services::get).toArray();
        return factoryType.getConstructor(servicesToInject).newInstance(servicesInstance);
    }
}
