package io.logz.sawmill.executor.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.parser.ConditionDefinition;
import io.logz.sawmill.parser.ConditionalExecutionStepDefinition;
import io.logz.sawmill.parser.ExecutionStepDefinition;
import io.logz.sawmill.parser.PipelineDefinition;
import io.logz.sawmill.parser.ProcessorDefinition;
import io.logz.sawmill.parser.ProcessorExecutionStepDefinition;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.logz.sawmill.utilities.JsonUtils.toJsonString;

public class ExecutionPlanDefinitionJsonParser {

    public ExecutionPlanDefinition parse(String config) {
        String configJson = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
        Map<String, Object> configMap = JsonUtils.fromJsonString(new TypeReference<Map<String, Object>>() {}, configJson);
        return parse(configMap);
    }

    private ExecutionPlanDefinition parse(Map<String, Object> configMap) {
        return null;
    }

    private String getTheOnlyKeyFrom(Map<String, Object> map) {
        Set<String> keys = map.keySet();
        if (keys.size() != 1) {
            throw new RuntimeException("JSON should contain only one key: " + toJsonString(map));
        }
        return keys.iterator().next();
    }

    private Boolean getBoolean(Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, Boolean.class, requiredField);
    }

    private String getString(Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, String.class, requiredField);
    }

    private List<Map<String, Object>> getList(Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, List.class, requiredField);
    }

    private Map<String, Object> getMap(Map<String, Object> map, String key, boolean requiredField) {
        return getValueAs(map, key, Map.class, requiredField);
    }

    private <T> T getValueAs(Map<String, Object> map, String key, Class<T> clazz, boolean requiredField) {
        Object value = map.get(key);
        if (value == null) {
            if (!requiredField) return null;
            throw new RuntimeException("\"" + key + "\"" + " is a required field which does not exists in " + map);
        }
        if (!clazz.isInstance(value)) {
            throw new RuntimeException("Value of field \"" + key + "\"" + " is: " + value  + " with type: " + value.getClass().getSimpleName() +
                    " , while it should be of type " + clazz.getSimpleName());
        }
        return clazz.cast(value);
    }

    private boolean isValueList(Map<String, Object> map, String key) {
        return map.get(key) instanceof List;
    }

    private boolean isValueMap(Map<String, Object> map, String key) {
        return map.get(key) instanceof Map;
    }

}
