package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;
import java.util.function.Function;
import java.util.regex.Pattern;

import static java.util.Objects.requireNonNull;

@ConditionProvider(type = "matchRegex", factory = MatchRegexCondition.Factory.class)
public class MatchRegexCondition implements Condition {

    private String field;
    private Pattern pattern;
    private Function<String, Boolean> matchingFunction;

    public MatchRegexCondition(String field, String regex, boolean caseInsensitive, boolean matchPartOfValue) {
        int patternFlags = caseInsensitive ? Pattern.CASE_INSENSITIVE : 0;
        this.field = requireNonNull(field);
        this.pattern = Pattern.compile(requireNonNull(regex), patternFlags);
        this.matchingFunction = matchPartOfValue ? this::matchPartOfValue : this::matchEntireOfValue;
    }

    @Override
    public boolean evaluate(Doc doc) {
        if (!doc.hasField(field)) return false;

        try {
            String value = doc.getField(field);
            return matchingFunction.apply(value);
        } catch (Exception e) {
            return false;
        }
    }

    private boolean matchEntireOfValue(String value) {
        return pattern.matcher(value).matches();
    }

    private boolean matchPartOfValue(String value) {
        return pattern.matcher(value).find();
    }

    public static class Factory implements Condition.Factory {

        public Factory() {}

        @Override
        public MatchRegexCondition create(Map<String, Object> config, ConditionParser conditionParser) {
            MatchRegexCondition.Configuration configuration = JsonUtils.fromJsonMap(MatchRegexCondition.Configuration.class, config);
            return new MatchRegexCondition(configuration.getField(), configuration.getRegex(), configuration.isCaseInsensitive(), configuration.isMatchPartOfValue());
        }

    }

    public static class Configuration {

        private String field;
        private String regex;
        private boolean caseInsensitive;
        private boolean matchPartOfValue = true;

        public Configuration() {}

        public Configuration(String field, String regex, boolean caseInsensitive, boolean matchPartOfValue) {
            this.field = field;
            this.regex = regex;
            this.caseInsensitive = caseInsensitive;
            this.matchPartOfValue = matchPartOfValue;
        }

        public String getField() {
            return field;
        }

        public String getRegex() {
            return regex;
        }

        public boolean isCaseInsensitive() {
            return caseInsensitive;
        }

        public boolean isMatchPartOfValue() {
            return matchPartOfValue;
        }

    }

}
