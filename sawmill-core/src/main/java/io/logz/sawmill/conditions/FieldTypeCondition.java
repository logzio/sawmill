package io.logz.sawmill.conditions;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.logz.sawmill.Condition;
import io.logz.sawmill.Doc;
import io.logz.sawmill.annotations.ConditionProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.parser.ConditionParser;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

@ConditionProvider(type="fieldType", factory = FieldTypeCondition.Factory.class)
public class FieldTypeCondition implements Condition {

    private final String path;
    private final String type;
    private static final ImmutableMap<String, Predicate<Object>> typeEvaluators = ImmutableMap.of(
            "string", value -> value instanceof String,
            "long", value -> value instanceof Long || value instanceof Integer,
            "double", value -> value instanceof Double || value instanceof Float,
            "list", value -> value instanceof List,
            "jsonobject", value -> value instanceof Map);

    public FieldTypeCondition(String path, String type) {
        if (path == null) {
            throw new ProcessorConfigurationException("failed to parse fieldType condition, could not resolve field path");
        }

        if (type == null) {
            throw new ProcessorConfigurationException("failed to parse fieldType condition, could not resolve field type");
        }

        ImmutableSet<String> supportedTypes = typeEvaluators.keySet();
        if (!supportedTypes.contains(type.toLowerCase())) throw new ProcessorConfigurationException("type ["+type+"] must be one of " + supportedTypes);

        this.path = path;
        this.type = type;
    }

    @Override
    public boolean evaluate(Doc doc) {
        return doc.hasField(path) && typeEvaluators.get(type.toLowerCase()).test(doc.getField(path));
    }

    public static class Factory implements Condition.Factory {

        @Override
        public Condition create(Map<String, Object> config, ConditionParser conditionParser) {
            Configuration configuration = JsonUtils.fromJsonMap(Configuration.class, config);
            String path = configuration.getPath();
            String type = configuration.getType();

            return new FieldTypeCondition(path, type);
        }
    }

    public static class Configuration {
        private String path;
        private String type;

        public String getPath() {
            return path;
        }

        public String getType() {
            return type;
        }
    }
}
