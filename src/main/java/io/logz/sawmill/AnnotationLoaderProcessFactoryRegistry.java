package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.annotations.ProcessProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.concurrent.TimeUnit.SECONDS;

public class AnnotationLoaderProcessFactoryRegistry {
    private static final Logger logger = LoggerFactory.getLogger(AnnotationLoaderProcessFactoryRegistry.class);

    public static void loadAnnotatedProcesses(ProcessFactoryRegistry processFactoryRegistry) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        int processesLoaded = 0;
        Reflections reflections = new Reflections("io.logz.sawmill");
        Set<Class<?>> processes =  reflections.getTypesAnnotatedWith(ProcessProvider.class);
        for (Class<?> process : processes) {
            try {
                String typeName = process.getAnnotation(ProcessProvider.class).type();
                processFactoryRegistry.register(typeName, (Process.Factory) process.getConstructor().newInstance());
                logger.info("{} process factory loaded successfully, took {}s", typeName, stopwatch.elapsed(SECONDS) - timeElapsed);
                processesLoaded++;
            } catch (Exception e) {
                logger.error("failed to load process {}", process.getName(), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(SECONDS);
            }
        }
        logger.info("{} process factories loaded, took {}s", processesLoaded, stopwatch.elapsed(SECONDS));
    }
}
