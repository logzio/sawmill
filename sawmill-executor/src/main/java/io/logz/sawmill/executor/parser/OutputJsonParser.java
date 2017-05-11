package io.logz.sawmill.executor.parser;

import java.util.Map;

import static io.logz.sawmill.utilities.JsonUtils.getMap;
import static io.logz.sawmill.utilities.JsonUtils.getTheOnlyKeyFrom;

public class OutputJsonParser {

    public OutputDefinition parse(Map<String, Object> configMap) {
        Map<String, Object> outputMap = getMap(configMap, "output", true);
        String type = getTheOnlyKeyFrom(outputMap);
        Map<String, Object> outputConfig = getMap(outputMap, type, true);

        return new OutputDefinition(type, outputConfig);
    }
}
