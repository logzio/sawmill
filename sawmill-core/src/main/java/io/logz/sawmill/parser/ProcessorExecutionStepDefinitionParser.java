package io.logz.sawmill.parser;

import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

public class ProcessorExecutionStepDefinitionParser {
    public static ProcessorExecutionStepDefinition parse(String processorType, Map<String, Object> config) {
        String name = JsonUtils.getString(config, "name", false);
        Map<String, Object> processorConfig = JsonUtils.getMap(config, "config", true);
        ProcessorDefinition processorDefinition = new ProcessorDefinition(processorType, processorConfig);

        List<Map<String, Object>> onFailure = JsonUtils.getList(config, "onFailure", false);
        List<ExecutionStepDefinition> onFailureExecutionStepDefinitions = ExecutionStepDefinitionParser.parse(onFailure);

        List<Map<String, Object>> onSuccess = JsonUtils.getList(config, "onSuccess", false);
        List<ExecutionStepDefinition> onSuccessExecutionStepDefinitions = ExecutionStepDefinitionParser.parse(onSuccess);

        return new ProcessorExecutionStepDefinition(processorDefinition, name, onFailureExecutionStepDefinitions, onSuccessExecutionStepDefinitions);
    }
}
