package io.logz.sawmill.parser;

import java.util.Map;

public class ConditionDefinition {
    private String type;
    private Map<String, Object> config;

    public ConditionDefinition() {}

    public ConditionDefinition(String type, Map<String, Object> config) {
        this.type = type;
        this.config = config;
    }

    public String getType() {
        return type;
    }

    public Map<String, Object> getConfig() {
        return config;
    }
}
