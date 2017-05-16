package io.logz.sawmill.conditions;

import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

import static java.util.Objects.requireNonNull;

@ConditionProvider(type = "mathComparator", factory = MathComparatorCondition.Factory.class)
public class MathComparatorCondition implements Condition {

    private final String field;
    private final Double gte;
    private final Double gt;
    private final Double lte;
    private final Double lt;

    public MathComparatorCondition(String field, Double gte, Double gt, Double lte, Double lt) {
        this.field = requireNonNull(field);
        this.gte = gte;
        this.gt = gt;
        this.lte = lte;
        this.lt = lt;
    }

    @Override
    public boolean evaluate(Doc doc) {
        if (!doc.hasField(field, Number.class)) return false;
        double value = ((Number)doc.getField(this.field)).doubleValue();

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
        private Double gte;
        private Double gt;
        private Double lte;
        private Double lt;

        public Configuration() {}

        public String getField() {
            return field;
        }

        public Double getGte() {
            return gte;
        }

        public Double getGt() {
            return gt;
        }

        public Double getLte() {
            return lte;
        }

        public Double getLt() {
            return lt;
        }
    }

}
