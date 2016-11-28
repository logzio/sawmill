package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.annotations.ProcessorProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ProcessorFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorFactoriesLoader.class);
    private static ProcessorFactoriesLoader instance;
    private final Reflections reflections;

    private ProcessorFactoriesLoader() {
        reflections = new Reflections("io.logz.sawmill");
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
        Set<Class<?>> processors =  reflections.getTypesAnnotatedWith(ProcessorProvider.class);
        for (Class<?> processor : processors) {
            try {
                ProcessorProvider annotation = processor.getAnnotation(ProcessorProvider.class);
                String typeName = annotation.type();
                Class typeFactory = annotation.factory();
                processorFactoryRegistry.register(typeName, (Processor.Factory) typeFactory.getConstructor().newInstance());
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
}
