package io.logz.sawmill.parser;

import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ExecutionStepDefinitionParser {
    public static ExecutionStepDefinition parse(Map<String, Object> configMap) {
        String type = JsonUtils.getTheOnlyKeyFrom(configMap);
        Map<String, Object> executionStepConfig = JsonUtils.getMap(configMap, type, true);

        if (type.equals("if")) {
            return ConditionalExecutionStepDefinitionParser.parse(executionStepConfig);
        }
        return ProcessorExecutionStepDefinitionParser.parse(type, executionStepConfig);
    }

    public static List<ExecutionStepDefinition> parse(List<Map<String, Object>> configMapList) {
        if (configMapList == null) return null;

        return configMapList.stream().map(ExecutionStepDefinitionParser::parse).collect(Collectors.toList());
    }
}
