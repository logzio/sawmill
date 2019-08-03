package io.logz.sawmill.conditions;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import javax.inject.Inject;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ConditionProvider(type = "hasValue", factory = FieldHasValueCondition.Factory.class)
public class FieldHasValueCondition implements Condition {

    private String field;
    private List<Object> possibleValues;

    public FieldHasValueCondition(String field, List<Object> possibleValues) {
        this.field = field;
        this.possibleValues = possibleValues;
    }

    @Override
    public boolean evaluate(Doc doc) {
        if (!doc.hasField(field)) return false;
        Object value = getValueFromDoc(doc);

        return possibleValues.stream()
                .map(possibleValue -> {
                    if (possibleValue instanceof Template) {
                        return ((Template) possibleValue).render(doc);
                    } else {
                        return possibleValue;
                    }
                }).anyMatch(value::equals);
    }

    private Object getValueFromDoc(Doc doc) {
        Object value = doc.getField(field);

        if (value instanceof Float)
            return new Float((float)value).doubleValue();
        else if (value instanceof Integer)
            return new Integer((int)value).longValue();
        return value;
    }

    public static class Factory implements Condition.Factory {
        private final TemplateService templateService;

        @Inject
        public Factory(TemplateService templateService) {
            this.templateService = requireNonNull(templateService);
        }

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            FieldHasValueCondition.Configuration fieldHasValueConfig = JsonUtils.fromJsonMap(FieldHasValueCondition.Configuration.class, config);
            List<Object> possibleValues = fieldHasValueConfig.getPossibleValues().stream()
                    .map(value -> {
                        if (value instanceof String) {
                            return templateService.createTemplate((String)value);
                        } else if (value instanceof Integer) {
                            return Longs.tryParse(value.toString());
                        } else if (value instanceof Float) {
                            return Doubles.tryParse(value.toString());
                        } else {
                            return value;
                        }
                    }).collect(Collectors.toList());
            return new FieldHasValueCondition(fieldHasValueConfig.getField(), possibleValues);
        }
    }

    public static class Configuration {
        private String field;
        private List<Object> possibleValues;

        public Configuration() {
        }

        public Configuration(String field, List<Object> possibleValues) {
            this.field = field;
            this.possibleValues = possibleValues;
        }

        public String getField() {
            return field;
        }

        public List<Object> getPossibleValues() {
            return possibleValues;
        }
    }
}
