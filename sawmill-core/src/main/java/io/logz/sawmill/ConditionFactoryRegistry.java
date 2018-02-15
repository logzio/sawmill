package io.logz.sawmill;

import io.logz.sawmill.exceptions.ProcessorMissingException;

import java.util.HashMap;
import java.util.Map;

public class ConditionFactoryRegistry {
    private final Map<String, Condition.Factory> conditionFactory = new HashMap<>();

    private static final ConditionFactoryRegistry conditionFactoryRegistry = new ConditionFactoryRegistry();

    private ConditionFactoryRegistry() {
        ConditionalFactoriesLoader.getInstance().loadAnnotatedProcessors(this);
    }

    public static ConditionFactoryRegistry getInstance() {
        return conditionFactoryRegistry;
    }

    public void register(String name, Condition.Factory factory) {
        conditionFactory.put(name, factory);
    }

    public Condition.Factory get(String name) {
        Condition.Factory factory = conditionFactory.get(name);
        if (factory == null) throw new ProcessorMissingException("No condition registered with name " + name);
        return factory;
    }
}
