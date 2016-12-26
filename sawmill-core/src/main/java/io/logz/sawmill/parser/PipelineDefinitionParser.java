package io.logz.sawmill.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static io.logz.sawmill.utilities.JsonUtils.toJsonString;

/**
 * Created by naorguetta on 18/12/2016.
 */
public class PipelineDefinitionParser {

    public PipelineDefinition parse(String config) {
        String configJson = ConfigFactory.parseString(config).root().render(ConfigRenderOptions.concise());
        Map<String, Object> configMap = JsonUtils.fromJsonString(new TypeReference<Map<String, Object>>() {}, configJson);
        return parse(configMap);
    }

    private PipelineDefinition parse(Map<String, Object> configMap) {
        String name = getString(configMap, "name", true);
        String description = getString(configMap, "description", true);
        Boolean ignoreFailure = getBoolean(configMap, "ignoreFailure", false);

        List<Map<String, Object>> executionSteps = getList(configMap, "executionSteps", true);
        List<ExecutionStepDefinition> executionStepDefinitionList = parseExecutionSteps(executionSteps);

        return new PipelineDefinition(name, description, executionStepDefinitionList, ignoreFailure);
    }

    private List<ExecutionStepDefinition> parseExecutionSteps(List<Map<String, Object>> configMapList) {
        if (configMapList == null) return null;

        return configMapList.stream().map(this::parseExecutionStep).collect(Collectors.toList());
    }

    private ExecutionStepDefinition parseExecutionStep(Map<String, Object> configMap) {
        String type = getTheOnlyKeyFrom(configMap);
        Map<String, Object> executionStepConfig = getMap(configMap, type, true);

        if (type.equals("if")) {
            return parseConditionalExecutionStep(executionStepConfig);
        }
        return parseProcessorExecutionStep(type, executionStepConfig);
    }

    private ConditionalExecutionStepDefinition parseConditionalExecutionStep(Map<String, Object> config) {
        Map<String, Object> condition = getMap(config, "condition", true);
        List<Map<String, Object>> onTrue = getList(config, "then", true);
        List<Map<String, Object>> onFalse = getList(config, "else", false);

        ConditionDefinition conditionDefinition = parseCondition(condition);
        List<ExecutionStepDefinition> onTrueDefinitions = parseExecutionSteps(onTrue);
        List<ExecutionStepDefinition> onFalseDefinitions = parseExecutionSteps(onFalse);
        return new ConditionalExecutionStepDefinition(conditionDefinition, onTrueDefinitions, onFalseDefinitions);
    }

    private ConditionDefinition parseCondition(Map<String, Object> condition) {
        String conditionType = getTheOnlyKeyFrom(condition);
        if (isValueList(condition, conditionType)) {
            // operands (and, or, not etc.)
            List<Map<String, Object>> configList = getList(condition, conditionType, true);
            List<ConditionDefinition> conditions = configList.stream().map(this::parseCondition).collect(Collectors.toList());
            return new ConditionDefinition(conditionType, ImmutableMap.of("conditions", conditions));
        }
        if (isValueMap(condition, conditionType)) {
            // terms (exists, hasValues etc.)
            Map<String, Object> configMap = getMap(condition, conditionType, true);
            return new ConditionDefinition(conditionType, configMap);
        }
        throw new RuntimeException(conditionType + " should be list or map");

    }

    private ProcessorExecutionStepDefinition parseProcessorExecutionStep(String processorType, Map<String, Object> config) {
        String name = getString(config, "name", true);
        Map<String, Object> processorConfig = getMap(config, "config", true);
        ProcessorDefinition processorDefinition = new ProcessorDefinition(processorType, processorConfig);

        List<Map<String, Object>> onFailure = getList(config, "onFailure", false);
        List<OnFailureExecutionStepDefinition> onFailureExecutionStepDefinitions = parseOnFailureExecutionSteps(onFailure);

        return new ProcessorExecutionStepDefinition(processorDefinition, name, onFailureExecutionStepDefinitions);
    }

    private List<OnFailureExecutionStepDefinition> parseOnFailureExecutionSteps(List<Map<String, Object>> configMapList) {
        if (configMapList == null) return null;

        return configMapList.stream().map(this::parseOnFailureExecutionStep).collect(Collectors.toList());
    }

    private OnFailureExecutionStepDefinition parseOnFailureExecutionStep(Map<String, Object> configMap) {
        String processorType = getTheOnlyKeyFrom(configMap);
        Map<String, Object> onFailureConfig = getMap(configMap, processorType, true);
        Map<String, Object> processorConfig = getMap(onFailureConfig, "config", true);
        ProcessorDefinition processorDefinition = new ProcessorDefinition(processorType, processorConfig);

        String name = getString(onFailureConfig, "name", true);
        return new OnFailureExecutionStepDefinition(processorDefinition, name);
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
            throw new RuntimeException("\"" + key + "\"" + " is a required field");
        }
        if (!clazz.isInstance(value)) {
            throw new RuntimeException("Value of field \"" + key + "\"" + " should be " + clazz.getSimpleName());
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
