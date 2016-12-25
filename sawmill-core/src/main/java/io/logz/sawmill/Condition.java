package io.logz.sawmill;

import io.logz.sawmill.parser.ConditionParser;

import java.util.Map;

/**
 * Created by naorguetta on 13/12/2016.
 */
public interface Condition {
    boolean evaluate(Doc doc);

    interface Factory {
        Condition create(Map<String, Object> config, ConditionParser conditionParser);
    }

}
