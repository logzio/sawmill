package io.logz.sawmill.executor;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.executor.annotations.InputProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class InputFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(InputFactoriesLoader.class);
    private static InputFactoriesLoader instance;
    private final Reflections reflections;

    private InputFactoriesLoader() {
        reflections = new Reflections("io.logz.sawmill.executor");
    }

    public static InputFactoriesLoader getInstance() {
        if (instance == null) {
            instance = new InputFactoriesLoader();
        }

        return instance;
    }

    public void loadAnnotatedInputs(InputFactoryRegistry inputFactoryRegistry) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        int inputsLoaded = 0;
        Set<Class<?>> inputs =  reflections.getTypesAnnotatedWith(InputProvider.class);
        for (Class<?> input : inputs) {
            try {
                InputProvider annotation = input.getAnnotation(InputProvider.class);
                String typeName = annotation.type();
                Class<? extends Input.Factory> typeFactory = annotation.factory();
                inputFactoryRegistry.register(typeName, typeFactory.getConstructor().newInstance());
                logger.info("{} input factory loaded successfully, took {}ms", typeName, stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                inputsLoaded++;
            } catch (Exception e) {
                logger.error("failed to load input {}", input.getName(), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(MILLISECONDS);
            }
        }
        logger.info("{} inputs factories loaded, took {}ms", inputsLoaded, stopwatch.elapsed(MILLISECONDS));
    }
}
