package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionDefinition;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Created by naorguetta on 13/12/2016.
 */
@ConditionProvider(type = "and", factory = AndCondition.Factory.class)
public class AndCondition implements Condition {

    private List<Condition> conditions;

    public AndCondition(List<Condition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean evaluate(Doc doc) {
        return conditions.stream().allMatch(condition -> condition.evaluate(doc));
    }

    public List<Condition> getConditions() {
        return conditions;
    }

    public static class Factory implements Condition.Factory {
        public Factory() {
        }

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            List<ConditionDefinition> conditionConfigList = (List<ConditionDefinition>) config.get("conditions");

            List<Condition> conditions = conditionConfigList.stream().map(conditionParser::parse).collect(Collectors.toList());

            return new AndCondition(conditions);
        }
    }
}
