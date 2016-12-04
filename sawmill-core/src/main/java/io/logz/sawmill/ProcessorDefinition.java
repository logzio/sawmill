package io.logz.sawmill;

import java.util.List;
import java.util.Map;

public class ProcessorDefinition {
    private String type;
    private String name;
    private Map<String,Object> config;
    private List<ProcessorDefinition> onFailure;

    public ProcessorDefinition() { }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public Map<String, Object> getConfig() {
        return config;
    }

    public List<ProcessorDefinition> getOnFailure() {
        return onFailure;
    }
}
