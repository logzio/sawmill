package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.annotations.ConditionProvider;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ConditionalFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(ConditionalFactoriesLoader.class);
    private static ConditionalFactoriesLoader instance;
    private final Reflections reflections;
    private final Map<Class<?>, Object> services;

    private ConditionalFactoriesLoader() {
        this(new TemplateService());
    }

    private ConditionalFactoriesLoader(TemplateService templateService) {
        reflections = new Reflections("io.logz.sawmill");
        services = new HashMap<>();
        services.put(TemplateService.class, templateService);
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
                ConditionProvider conditionProvider = condition.getAnnotation(ConditionProvider.class);
                String typeName = conditionProvider.type();
                conditionFactoryRegistry.register(typeName, getFactory(conditionProvider));
                logger.debug("{} condition factory loaded successfully, took {}ms", typeName, stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                conditionsLoaded++;
            } catch (Exception e) {
                logger.error("failed to load condition {}", condition.getName(), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(MILLISECONDS);
            }
        }
        logger.debug("{} conditions factories loaded, took {}ms", conditionsLoaded, stopwatch.elapsed(MILLISECONDS));
    }

    public Condition.Factory getFactory(ConditionProvider conditionProvider) throws InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, NoSuchMethodException {
        Class<? extends Condition.Factory> factoryType = conditionProvider.factory();
        Optional<? extends Constructor<?>> injectConstructor = Stream.of(factoryType.getConstructors())
                .filter(constructor -> constructor.isAnnotationPresent(Inject.class)).findFirst();
        if (injectConstructor.isPresent()) {
            Class<?>[] servicesToInject = injectConstructor.get().getParameterTypes();
            Object[] servicesInstance = Stream.of(servicesToInject).map(services::get).toArray();
            return factoryType.getConstructor(servicesToInject).newInstance(servicesInstance);
        } else {
            return factoryType.getConstructor().newInstance();
        }
    }
}
