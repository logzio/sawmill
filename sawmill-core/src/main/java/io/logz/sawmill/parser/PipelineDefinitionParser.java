package io.logz.sawmill.parser;

import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.ImmutableMap;
import com.typesafe.config.ConfigFactory;
import com.typesafe.config.ConfigRenderOptions;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

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
        String name = getString(configMap, "name");
        String description = getString(configMap, "description");
        Boolean ignoreFailure = (Boolean) configMap.get("ignoreFailure");

        List<Map<String, Object>> executionSteps = getList(configMap, "executionSteps");
        List<ExecutionStepDefinition> executionStepDefinitionList = parseExecutionSteps(executionSteps);

        return new PipelineDefinition(name, description, executionStepDefinitionList, ignoreFailure);
    }

    private List<ExecutionStepDefinition> parseExecutionSteps(List<Map<String, Object>> configMapList) {
        return configMapList.stream().map(this::parseExecutionStep).collect(Collectors.toList());
    }

    private ExecutionStepDefinition parseExecutionStep(Map<String, Object> configMap) {
        String type = configMap.keySet().iterator().next();
        Map<String, Object> executionStepConfig = getMap(configMap, type);

        if (type.equals("if")) {
            return parseConditionalExecutionStep(executionStepConfig);
        }
        return parseProcessorExecutionStep(type, executionStepConfig);
    }

    private ConditionalExecutionStepDefinition parseConditionalExecutionStep(Map<String, Object> config) {
        ConditionDefinition condition = parseCondition(getMap(config, "condition"));
        List<Map<String, Object>> onTrue = getList(config, "then");
        List<Map<String, Object>> onFalse = getList(config, "else");

        return new ConditionalExecutionStepDefinition(condition, parseExecutionSteps(onTrue), parseExecutionSteps(onFalse));
    }

    private ConditionDefinition parseCondition(Map<String, Object> condition) {
        String type = condition.keySet().iterator().next();
        try {
            // operand (and, or, not etc.)
            List<Map<String, Object>> configList = getList(condition, type);
            List<ConditionDefinition> conditions = configList.stream().map(this::parseCondition).collect(Collectors.toList());
            return new ConditionDefinition(type, ImmutableMap.of("conditions", conditions));
        } catch (Exception e) {
            // terms (exists, hasValues etc.)
            return new ConditionDefinition(type, getMap(condition, type));
        }
    }

    private ProcessorExecutionStepDefinition parseProcessorExecutionStep(String processorType, Map<String, Object> config) {
        String name = getString(config, "name");
        Map<String, Object> processorConfig = getMap(config, "config");
        ProcessorDefinition processorDefinition = new ProcessorDefinition(processorType, processorConfig);

        List<Map<String, Object>> onFailure = getList(config, "onFailure");
        List<OnFailureExecutionStepDefinition> onFailureExecutionStepDefinitions = parseOnFailureExecutionSteps(onFailure);

        return new ProcessorExecutionStepDefinition(processorDefinition, name, onFailureExecutionStepDefinitions);
    }

    private List<OnFailureExecutionStepDefinition> parseOnFailureExecutionSteps(List<Map<String, Object>> configMapList) {
        if (configMapList == null) return null;
        return configMapList.stream().map(this::parseOnFailureExecutionStep).collect(Collectors.toList());
    }

    private OnFailureExecutionStepDefinition parseOnFailureExecutionStep(Map<String, Object> configMap) {
        String processorType = configMap.keySet().iterator().next();
        Map<String, Object> onFailureConfig = getMap(configMap, processorType);
        Map<String, Object> processorConfig = getMap(onFailureConfig, "config");
        ProcessorDefinition processorDefinition = new ProcessorDefinition(processorType, processorConfig);

        String name = getString(onFailureConfig, "name");
        return new OnFailureExecutionStepDefinition(processorDefinition, name);
    }

    private String getString(Map<String, Object> map, String key) {
        return (String) map.get(key);
    }

    private List<Map<String, Object>> getList(Map<String, Object> map, String key) {
        return (List<Map<String, Object>>) map.get(key);
    }

    private Map<String, Object> getMap(Map<String, Object> map, String key) {
        return (Map<String, Object>) map.get(key);
    }

}
