package io.logz.sawmill.utils;

import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;

import java.util.LinkedHashMap;
import java.util.Map;

import static io.logz.sawmill.utils.FactoryUtils.createFactory;

public class FactoryUtils {
    public static <T extends Processor> T createProcessor(Class<? extends Processor> clazz, Object... objects) {
        Map<String, Object> config = createConfig(objects);
        return createProcessor(clazz, config);
    }

    public static <T extends Processor> T createProcessor(Class<? extends Processor> clazz, Map<String, Object> config) {
        Processor.Factory factory = createFactory(clazz);
        return (T) factory.create(config);
    }

    public static <T extends Processor.Factory> T createFactory(Class<? extends Processor> clazz) {
        ProcessorProvider annotation = clazz.getAnnotation(ProcessorProvider.class);
        try {
            return (T) annotation.factory().getConstructor().newInstance();
        } catch (Exception e) {
            throw new RuntimeException();
        }
    }

    public static Map<String, Object> createConfig(Object... objects) {
        LinkedHashMap<String, Object> map = new LinkedHashMap<>();
        if (objects != null) {
            for (int i = 0; i < objects.length; i++) {
                map.put((String) objects[i], objects[++i]);
            }
        }

        return map;
    }
}