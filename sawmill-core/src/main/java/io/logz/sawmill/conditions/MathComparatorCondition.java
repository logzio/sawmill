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

@ConditionProvider(type = "mathComparator", factory = MathComparatorCondition.Factory.class)
public class MathComparatorCondition implements Condition {

    private final String field;
    private final Long gte;
    private final Long gt;
    private final Long lte;
    private final Long lt;

    public MathComparatorCondition(String field, Long gte, Long gt, Long lte, Long lt) {
        this.field = requireNonNull(field);
        this.gte = gte;
        this.gt = gt;
        this.lte = lte;
        this.lt = lt;
    }

    @Override
    public boolean evaluate(Doc doc) {
        if (!doc.hasField(field, Number.class)) return false;
        Long value = ((Number)doc.getField(this.field)).longValue();

        boolean greaterThan = (gte == null || value >= gte) && (gt == null || value > gt);
        boolean lessThan = (lte == null || value <= lte) && (lt == null || value < lt);

        return greaterThan && lessThan;
    }

    public static class Factory implements Condition.Factory {

        public Factory() {}

        @Override
        public MathComparatorCondition create(Map<String, Object> config, ConditionParser conditionParser) {
            MathComparatorCondition.Configuration configuration = JsonUtils.fromJsonMap(MathComparatorCondition.Configuration.class, config);
            return new MathComparatorCondition(configuration.getField(),
                    configuration.getGte(),
                    configuration.getGt(),
                    configuration.getLte(),
                    configuration.getLt());
        }

    }

    public static class Configuration {

        private String field;
        private Long gte;
        private Long gt;
        private Long lte;
        private Long lt;

        public Configuration() {}

        public String getField() {
            return field;
        }

        public Long getGte() {
            return gte;
        }

        public Long getGt() {
            return gt;
        }

        public Long getLte() {
            return lte;
        }

        public Long getLt() {
            return lt;
        }
    }

}
