package io.logz.sawmill.executor.parser;

import java.util.Map;

public class OutputDefinition {
    private String type;
    private Map<String, Object> config;

    public OutputDefinition(String type, Map<String, Object> config) {
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
