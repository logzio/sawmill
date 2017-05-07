package io.logz.sawmill.executor.parser;

import java.util.Map;

import static io.logz.sawmill.utilities.JsonUtils.getMap;
import static io.logz.sawmill.utilities.JsonUtils.getTheOnlyKeyFrom;

public class InputJsonParser {

    public InputDefinition parse(Map<String, Object> configMap) {
        Map<String, Object> inputMap = getMap(configMap, "input", true);
        String type = getTheOnlyKeyFrom(inputMap);
        Map<String, Object> inputConfig = getMap(configMap, type, true);

        return new InputDefinition(type, inputConfig);
    }
}
