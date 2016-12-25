package io.logz.sawmill;

import io.logz.sawmill.exceptions.ProcessorMissingException;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by naorguetta on 18/12/2016.
 */
public class ConditionFactoryRegistry {
    private final Map<String, Condition.Factory> conditionFactory = new HashMap<>();

    public ConditionFactoryRegistry() { }

    public void register(String name, Condition.Factory factory) {
        conditionFactory.put(name, factory);
    }

    public Condition.Factory get(String name) {
        Condition.Factory factory = conditionFactory.get(name);
        if (factory == null) throw new ProcessorMissingException("No such condition with name " + name);
        return factory;
    }
}
