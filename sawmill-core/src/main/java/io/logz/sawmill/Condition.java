package io.logz.sawmill;

import io.logz.sawmill.parser.ConditionParser;

import java.util.Map;

public interface Condition {
    boolean evaluate(Doc doc);

    interface Factory {
        Condition create(Map<String, Object> config, ConditionParser conditionParser);
    }

}
