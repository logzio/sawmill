package io.logz.sawmill.parser;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class ConditionDefinitionParser {
    public static ConditionDefinition parse(Map<String, Object> condition) {
        String conditionType = JsonUtils.getTheOnlyKeyFrom(condition);
        if (JsonUtils.isValueList(condition, conditionType)) {
            // operands (and, or, not etc.)
            List<Map<String, Object>> configList = JsonUtils.getList(condition, conditionType, true);
            List<ConditionDefinition> conditions = configList.stream().map(ConditionDefinitionParser::parse).collect(Collectors.toList());
            return new ConditionDefinition(conditionType, ImmutableMap.of("conditions", conditions));
        }
        if (JsonUtils.isValueMap(condition, conditionType)) {
            // terms (exists, hasValues etc.)
            Map<String, Object> configMap = JsonUtils.getMap(condition, conditionType, true);
            return new ConditionDefinition(conditionType, configMap);
        }
        throw new RuntimeException(conditionType + " should be list or map");

    }
}
