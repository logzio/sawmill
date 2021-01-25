package io.logz.sawmill.utils;

import io.logz.sawmill.Condition;
import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.ConditionalFactoriesLoader;
import io.logz.sawmill.GeoIpConfiguration;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorFactoriesLoader;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.parser.ConditionParser;

import java.util.LinkedHashMap;
import java.util.Map;

public class FactoryUtils {

    public static final TemplateService templateService = new TemplateService();
    public static final ProcessorFactoryRegistry defaultProcessorFactoryRegistry = new ProcessorFactoryRegistry(
            new ProcessorFactoriesLoader(templateService, new GeoIpConfiguration("GeoLite2-City.mmdb.gz"))
    );
    public static final ConditionFactoryRegistry defaultConditionFactoryRegistry = new ConditionFactoryRegistry(
            new ConditionalFactoriesLoader(templateService)
    );
    public static final ConditionParser conditionParser = new ConditionParser(defaultConditionFactoryRegistry);


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
            return (T) defaultProcessorFactoryRegistry.get(annotation.type());
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
    }

    public static <T extends Condition.Factory> T createConditionFactory(Class<? extends Condition> clazz) {
        ConditionProvider annotation = clazz.getAnnotation(ConditionProvider.class);
        try {
            return (T) defaultConditionFactoryRegistry.get(annotation.type());
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