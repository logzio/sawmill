package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

public class TestCondition implements Condition {
    public final String value;

    public TestCondition(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public boolean evaluate(Doc doc) {
        return true;
    }

    public static class Factory implements Condition.Factory {
        public Factory() {
        }

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            Configuration testConditionConfig = JsonUtils.fromJsonMap(Configuration.class, config);

            return new TestCondition(testConditionConfig.getValue());
        }
    }

    public static class Configuration {
        private String value;

        public Configuration() {
        }

        public Configuration(String value) {
            this.value = value;
        }

        public String getValue() {
            return value;
        }
    }
}
