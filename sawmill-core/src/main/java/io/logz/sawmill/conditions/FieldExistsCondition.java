package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

@ConditionProvider(type = "exists", factory = FieldExistsCondition.Factory.class)
public class FieldExistsCondition implements Condition {
    private String field;

    public FieldExistsCondition(String field) {
        this.field = field;
    }

    @Override
    public boolean evaluate(Doc doc) {
        return doc.hasField(field);
    }

    public static class Factory implements Condition.Factory {
        public Factory() {
        }

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            FieldExistsCondition.Configuration existsCondition = JsonUtils.fromJsonMap(FieldExistsCondition.Configuration.class, config);

            return new FieldExistsCondition(existsCondition.getField());
        }
    }

    public static class Configuration {
        private String field;

        public Configuration() {
        }

        public Configuration(String field) {
            this.field = field;
        }

        public String getField() {
            return field;
        }

    }
}
