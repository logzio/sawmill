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
@ConditionProvider(type = "or", factory = OrCondition.Factory.class)
public class OrCondition implements Condition {

    private List<Condition> conditions;

    public OrCondition(List<Condition> conditions) {
        this.conditions = conditions;
    }

    @Override
    public boolean evaluate(Doc doc) {
        return conditions.stream().anyMatch(condition -> condition.evaluate(doc));
    }

    public static class Factory implements Condition.Factory {
        public Factory() {
        }

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            List<ConditionDefinition> conditionConfigList = (List<ConditionDefinition>) config.get("conditions");

            List<Condition> conditions = conditionConfigList.stream().map(conditionParser::parse).collect(Collectors.toList());

            return new OrCondition(conditions);
        }
    }

}
