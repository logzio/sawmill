package io.logz.sawmill.executor;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.executor.annotations.OutputProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class OutputFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(OutputFactoriesLoader.class);
    private static OutputFactoriesLoader instance;
    private final Reflections reflections;

    private OutputFactoriesLoader() {
        reflections = new Reflections("io.logz.sawmill.executor");
    }

    public static OutputFactoriesLoader getInstance() {
        if (instance == null) {
            instance = new OutputFactoriesLoader();
        }

        return instance;
    }

    public void loadAnnotatedOutputs(OutputFactoryRegistry outputFactoryRegistry) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        int outputsLoaded = 0;
        Set<Class<?>> outputs =  reflections.getTypesAnnotatedWith(OutputProvider.class);
        for (Class<?> output : outputs) {
            try {
                OutputProvider annotation = output.getAnnotation(OutputProvider.class);
                String typeName = annotation.type();
                Class<? extends Output.Factory> typeFactory = annotation.factory();
                outputFactoryRegistry.register(typeName, typeFactory.getConstructor().newInstance());
                logger.info("{} output factory loaded successfully, took {}ms", typeName, stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                outputsLoaded++;
            } catch (Exception e) {
                logger.error("failed to load output {}", output.getName(), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(MILLISECONDS);
            }
        }
        logger.info("{} outputs factories loaded, took {}ms", outputsLoaded, stopwatch.elapsed(MILLISECONDS));
    }
}
