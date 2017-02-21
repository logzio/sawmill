package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

@ConditionProvider(type = "in", factory = InCondition.Factory.class)
public class InCondition implements Condition {

    private String path;
    private String value;

    public InCondition(String path, String value) {
        this.path = path;
        this.value = value;
    }

    @Override
    public boolean evaluate(Doc doc) {
        if (!doc.hasField(path)) {
            return false;
        }

        Object pathValue = doc.getField(path);
        if (!(pathValue instanceof List)) {
            return false;
        }

        List<String> valuesList = (List<String>) pathValue;
        return valuesList.stream().anyMatch(value::equals);
    }

    public static class Factory implements Condition.Factory {
        public Factory() {
        }

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            Configuration configuration = JsonUtils.fromJsonMap(Configuration.class, config);
            return new InCondition(configuration.getPath(), configuration.getValue());
        }
    }

    public static class Configuration {
        private String path;
        private String value;

        public Configuration() {

        }


        public String getPath() {
            return path;
        }

        public String getValue() {
            return value;
        }
    }
}
