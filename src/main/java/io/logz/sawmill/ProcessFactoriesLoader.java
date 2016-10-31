package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.annotations.ProcessorProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ProcessFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(ProcessFactoriesLoader.class);
    private static ProcessFactoriesLoader instance;
    private final Reflections reflections;

    private ProcessFactoriesLoader() {
        reflections = new Reflections("io.logz.sawmill");
    }

    public static ProcessFactoriesLoader getInstance() {
        if (instance == null) {
            instance = new ProcessFactoriesLoader();
        }

        return instance;
    }

    public void loadAnnotatedProcesses(ProcessorFactoryRegistry processorFactoryRegistry) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        int processesLoaded = 0;
        Set<Class<?>> processes =  reflections.getTypesAnnotatedWith(ProcessorProvider.class);
        for (Class<?> process : processes) {
            try {
                String typeName = process.getAnnotation(ProcessorProvider.class).name();
                processorFactoryRegistry.register(typeName, (Processor.Factory) process.getConstructor().newInstance());
                logger.info("{} processor factory loaded successfully, took {}ms", typeName, stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                processesLoaded++;
            } catch (Exception e) {
                logger.error("failed to load process {}", process.getName(), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(MILLISECONDS);
            }
        }
        logger.info("{} processor factories loaded, took {}ms", processesLoaded, stopwatch.elapsed(MILLISECONDS));
    }
}
