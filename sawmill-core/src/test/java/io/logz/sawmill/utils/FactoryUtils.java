package io.logz.sawmill.utils;

import io.logz.sawmill.Condition;
import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.ConditionalFactoriesLoader;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorFactoriesLoader;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.parser.ConditionParser;

import java.util.LinkedHashMap;
import java.util.Map;

public class FactoryUtils {
    public static final TemplateService templateService = new TemplateService();
    public static final ConditionParser conditionParser;

    static {
        ConditionFactoryRegistry conditionFactoryRegistry = ConditionFactoryRegistry.getInstance();
        conditionParser = new ConditionParser(conditionFactoryRegistry);
    }

    public static <T extends Processor> T createProcessor(Class<? extends Processor> clazz, Object... objects) {
        Map<String, Object> config = createConfig(objects);
        return createProcessor(clazz, config);
    }

    public static <T extends Processor> T createProcessor(Class<? extends Processor> clazz, Map<String, Object> config) {
        Processor.Factory factory = createProcessorFactory(clazz);
        return (T) factory.create(config);
    }

    public static <T extends Condition> T createCondition(Class<? extends Condition> clazz, Object... objects) {
        Map<String, Object> config = createConfig(objects);
        return createCondition(clazz, config);
    }

    public static <T extends Condition> T createCondition(Class<? extends Condition> clazz, Map<String, Object> config) {
        Condition.Factory factory = createConditionFactory(clazz);
        return (T) factory.create(config, conditionParser);
    }

    public static <T extends Processor.Factory> T createProcessorFactory(Class<? extends Processor> clazz) {
        ProcessorProvider annotation = clazz.getAnnotation(ProcessorProvider.class);
        try {
            return (T) ProcessorFactoriesLoader.getInstance().getFactory(annotation);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Condition.Factory> T createConditionFactory(Class<? extends Condition> clazz) {
        ConditionProvider annotation = clazz.getAnnotation(ConditionProvider.class);
        try {
            return (T) ConditionalFactoriesLoader.getInstance().getFactory(annotation);
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