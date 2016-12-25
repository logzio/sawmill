package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

/**
 * Created by naorguetta on 13/12/2016.
 */
@ConditionProvider(type = "hasValue", factory = FieldHasValueCondition.Factory.class)
public class FieldHasValueCondition implements Condition {

    private String field;
    private List<String> possibleValues;

    public FieldHasValueCondition(String field, List<String> possibleValues) {
        this.field = field;
        this.possibleValues = possibleValues;
    }

    @Override
    public boolean evaluate(Doc doc) {
        if (!doc.hasField(field)) return false;

        Object value = doc.getField(this.field);

        return possibleValues.stream().anyMatch(value::equals);
    }

    public static class Factory implements Condition.Factory {
        public Factory() {
        }

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            FieldHasValueCondition.Configuration fieldHasValueConfig = JsonUtils.fromJsonMap(FieldHasValueCondition.Configuration.class, (Map) config);
            return new FieldHasValueCondition(fieldHasValueConfig.getField(), fieldHasValueConfig.getPossibleValues());
        }
    }

    public static class Configuration {
        private String field;
        private List<String> possibleValues;

        public Configuration() {
        }

        public Configuration(String field, List<String> possibleValues) {
            this.field = field;
            this.possibleValues = possibleValues;
        }

        public String getField() {
            return field;
        }

        public List<String> getPossibleValues() {
            return possibleValues;
        }
    }
}
