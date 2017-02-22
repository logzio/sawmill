package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;
import java.util.regex.Pattern;

@ConditionProvider(type = "matchRegex", factory = MatchRegexCondition.Factory.class)
public class MatchRegexCondition implements Condition {

    private String field;
    private Pattern pattern;

    public MatchRegexCondition(String field, String regex) {
        this.field = field;
        this.pattern = Pattern.compile(regex);
    }

    @Override
    public boolean evaluate(Doc doc) {
        if (!doc.hasField(field)) return false;

        try {
            String value = doc.getField(field);
            return pattern.matcher(value).matches();
        } catch (Exception e) {
            return false;
        }
    }

    public static class Factory implements Condition.Factory {

        public Factory() {}

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            MatchRegexCondition.Configuration configuration = JsonUtils.fromJsonMap(MatchRegexCondition.Configuration.class, config);
            return new MatchRegexCondition(configuration.getField(), configuration.getRegex());
        }

    }

    public static class Configuration {

        private String field;
        private String regex;

        public Configuration() {}

        public Configuration(String field, String regex) {
            this.field = field;
            this.regex = regex;
        }

        public String getField() {
            return field;
        }

        public String getRegex() {
            return regex;
        }

    }

}
