package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.parser.ConditionDefinition;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
            Configuration configuration = JsonUtils.fromJsonMap(Configuration.class, config);
            List<ConditionDefinition> conditionDefinitions = configuration.getConditions();
            if (conditionDefinitions == null || conditionDefinitions.size() == 0) {
                throw new ProcessorConfigurationException("'or' condition must contain a valid list of conditions, with at least one condition");
            }
            List<Condition> conditions = conditionDefinitions.stream().map(conditionParser::parse).collect(Collectors.toList());

            return new OrCondition(conditions);
        }
    }

    public static class Configuration {
        private List<ConditionDefinition> conditions;

        public Configuration() {
        }

        public List<ConditionDefinition> getConditions() {
            return conditions;
        }
    }

}
