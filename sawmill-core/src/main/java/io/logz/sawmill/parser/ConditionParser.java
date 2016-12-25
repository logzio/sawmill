package io.logz.sawmill.parser;

import io.logz.sawmill.Condition;
import io.logz.sawmill.ConditionFactoryRegistry;
import io.logz.sawmill.parser.ConditionDefinition;

/**
 * Created by naorguetta on 22/12/2016.
 */
public class ConditionParser {

    private ConditionFactoryRegistry conditionFactoryRegistry;

    public ConditionParser(ConditionFactoryRegistry conditionFactoryRegistry) {
        this.conditionFactoryRegistry = conditionFactoryRegistry;
    }

    public Condition parse(ConditionDefinition conditionDefinition) {
        Condition.Factory factory = conditionFactoryRegistry.get(conditionDefinition.getType());
        return factory.create(conditionDefinition.getConfig(), this);
    }
}
