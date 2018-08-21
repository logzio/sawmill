package io.logz.sawmill.parser;

import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

public class ConditionalExecutionStepDefinitionParser {
    public static ConditionalExecutionStepDefinition parse(Map<String, Object> config) {
        Map<String, Object> condition = JsonUtils.getMap(config, "condition", true);
        List<Map<String, Object>> onTrue = JsonUtils.getList(config, "then", true);
        List<Map<String, Object>> onFalse = JsonUtils.getList(config, "else", false);

        ConditionDefinition conditionDefinition = ConditionDefinitionParser.parse(condition);
        List<ExecutionStepDefinition> onTrueDefinitions = ExecutionStepDefinitionParser.parse(onTrue);
        List<ExecutionStepDefinition> onFalseDefinitions = ExecutionStepDefinitionParser.parse(onFalse);
        return new ConditionalExecutionStepDefinition(conditionDefinition, onTrueDefinitions, onFalseDefinitions);
    }
}
