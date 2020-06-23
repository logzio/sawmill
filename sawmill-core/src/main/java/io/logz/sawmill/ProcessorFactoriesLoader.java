package io.logz.sawmill;

import com.google.common.base.Stopwatch;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.SawmillException;
import org.reflections.Reflections;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Inject;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Stream;

import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class ProcessorFactoriesLoader {
    private static final Logger logger = LoggerFactory.getLogger(ProcessorFactoriesLoader.class);
    private final Reflections reflections;
    private final Map<Class<?>, Object> dependenciesToInject;

    public ProcessorFactoriesLoader(TemplateService templateService, SawmillConfiguration... sawmillConfigurations) {
        reflections = new Reflections("io.logz.sawmill");
        dependenciesToInject = new HashMap<>();
        dependenciesToInject.put(TemplateService.class, templateService);
        Arrays.stream(sawmillConfigurations).forEach(config -> dependenciesToInject.put(config.getClass(), config));
    }

    public void loadAnnotatedProcessors(ProcessorFactoryRegistry processorFactoryRegistry) {
        Stopwatch stopwatch = Stopwatch.createStarted();
        long timeElapsed = 0;

        int processorsLoaded = 0;
        Set<Class<?>> processors = reflections.getTypesAnnotatedWith(ProcessorProvider.class);
        for (Class<?> processor : processors) {
            try {
                ProcessorProvider processorProvider = processor.getAnnotation(ProcessorProvider.class);
                String typeName = processorProvider.type();
                Processor.Factory factory = getFactory(processorProvider);
                if (null != factory) {
                    processorFactoryRegistry.register(typeName, factory);
                    logger.debug("{} processor factory loaded successfully, took {}ms", typeName, stopwatch.elapsed(MILLISECONDS) - timeElapsed);
                    processorsLoaded++;
                }
            } catch (Exception e) {
                throw new SawmillException(String.format("failed to load processor %s", processor.getName()), e);
            }
            finally {
                timeElapsed = stopwatch.elapsed(MILLISECONDS);
            }
        }
        logger.debug("{} processor factories loaded, took {}ms", processorsLoaded, stopwatch.elapsed(MILLISECONDS));
    }

    public Processor.Factory getFactory(ProcessorProvider processorProvider) throws InstantiationException, IllegalAccessException, java.lang.reflect.InvocationTargetException, NoSuchMethodException {
        Class<? extends Processor.Factory> factoryType = processorProvider.factory();
        Optional<? extends Constructor<?>> injectConstructor = Stream.of(factoryType.getConstructors())
                .filter(constructor -> constructor.isAnnotationPresent(Inject.class)).findFirst();
        if (injectConstructor.isPresent()) {

            Class<?>[] servicesToInject = injectConstructor.get().getParameterTypes();
            if (Arrays.stream(servicesToInject).allMatch(
                    serviceType -> checkDependencyPresent(processorProvider, serviceType))) {

                Object[] servicesInstance = Stream.of(servicesToInject).map(dependenciesToInject::get).toArray();
                return factoryType.getConstructor(servicesToInject).newInstance(servicesInstance);
            }
            return null;
        } else {
            return factoryType.getConstructor().newInstance();
        }
    }

    private boolean checkDependencyPresent(ProcessorProvider processorProvider, Class<?> serviceType) {
        if (!dependenciesToInject.containsKey(serviceType)) {
            logger.warn(String.format(
                    "Could not instantiate %s processor factory, %s dependency missing",
                    processorProvider.factory().getName(),
                    serviceType.getSimpleName()
            ));
            return false;
        }
        return true;
    }
}
