package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.annotations.ConditionProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Set;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ConditionalFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConditionalFactoriesLoader.class);
    private static ConditionalFactoriesLoader instance;
    private final Reflections reflections;

    private ConditionalFactoriesLoader() {
        reflections = new Reflections("io.logz.sawmill");
    }

    public static ConditionalFactoriesLoader getInstance() {
        if (instance == null) {
            instance = new ConditionalFactoriesLoader();
        }

        return instance;
    }

    public void loadAnnotatedProcessors(ConditionFactoryRegistry conditionFactoryRegistry) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        int conditionsLoaded = 0;
        Set<Class<?>> conditions =  reflections.getTypesAnnotatedWith(ConditionProvider.class);
        for (Class<?> condition : conditions) {
            try {
                ConditionProvider annotation = condition.getAnnotation(ConditionProvider.class);
                String typeName = annotation.type();
                Class<? extends Condition.Factory> typeFactory = annotation.factory();
                conditionFactoryRegistry.register(typeName, typeFactory.getConstructor().newInstance());
                logger.info("{} condition factory loaded successfully, took {}ms", typeName, stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                conditionsLoaded++;
            } catch (Exception e) {
                logger.error("failed to load condition {}", condition.getName(), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(MILLISECONDS);
            }
        }
        logger.info("{} conditions factories loaded, took {}ms", conditionsLoaded, stopwatch.elapsed(MILLISECONDS));
    }
}
