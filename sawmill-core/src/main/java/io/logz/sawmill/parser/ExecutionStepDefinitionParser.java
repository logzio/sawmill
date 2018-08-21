package io.logz.sawmill.parser;

import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecutionStepDefinitionParser {
    public static List<ExecutionStepDefinition> parse(List<Map<String, Object>> configMapList) {
        if (configMapList == null) return null;

        return configMapList.stream().map(ExecutionStepDefinitionParser::parse).collect(Collectors.toList());
    }

    private static ExecutionStepDefinition parse(Map<String, Object> configMap) {
        String type = JsonUtils.getTheOnlyKeyFrom(configMap);
        Map<String, Object> executionStepConfig = JsonUtils.getMap(configMap, type, true);

        if (type.equals("if")) {
            return parseConditional(executionStepConfig);
        }
        return parseProcessor(type, executionStepConfig);
    }

    private static ConditionalExecutionStepDefinition parseConditional(Map<String, Object> config) {
        Map<String, Object> condition = JsonUtils.getMap(config, "condition", true);
        List<Map<String, Object>> onTrue = JsonUtils.getList(config, "then", true);
        List<Map<String, Object>> onFalse = JsonUtils.getList(config, "else", false);

        ConditionDefinition conditionDefinition = ConditionDefinitionParser.parse(condition);
        List<ExecutionStepDefinition> onTrueDefinitions = parse(onTrue);
        List<ExecutionStepDefinition> onFalseDefinitions = parse(onFalse);
        return new ConditionalExecutionStepDefinition(conditionDefinition, onTrueDefinitions, onFalseDefinitions);
    }

    private static ProcessorExecutionStepDefinition parseProcessor(String processorType, Map<String, Object> config) {
        String name = JsonUtils.getString(config, "name", false);
        Map<String, Object> processorConfig = JsonUtils.getMap(config, "config", true);
        ProcessorDefinition processorDefinition = new ProcessorDefinition(processorType, processorConfig);

        List<Map<String, Object>> onFailure = JsonUtils.getList(config, "onFailure", false);
        List<ExecutionStepDefinition> onFailureExecutionStepDefinitions = parse(onFailure);

        List<Map<String, Object>> onSuccess = JsonUtils.getList(config, "onSuccess", false);
        List<ExecutionStepDefinition> onSuccessExecutionStepDefinitions = parse(onSuccess);

        return new ProcessorExecutionStepDefinition(processorDefinition, name, onFailureExecutionStepDefinitions, onSuccessExecutionStepDefinitions);
    }
}
