package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

/**
 * Created by naorguetta on 19/12/2016.
 */
@ConditionProvider(type = "exists", factory = ExistsCondition.Factory.class)
public class ExistsCondition implements Condition {
    private String field;

    public ExistsCondition(String field) {
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
            ExistsCondition.Configuration existsCondition = JsonUtils.fromJsonMap(ExistsCondition.Configuration.class, config);

            return new ExistsCondition(existsCondition.getField());
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
