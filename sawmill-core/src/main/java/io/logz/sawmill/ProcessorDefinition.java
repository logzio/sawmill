package io.logz.sawmill;

import com.fasterxml.jackson.annotation.JsonSetter;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;
import java.util.Map;

public class ProcessorDefinition {
    private String type;
    private String name;
    private String config;
    private List<ProcessorDefinition> onFailure;

    public ProcessorDefinition() { }

    public String getType() {
        return type;
    }

    public String getName() {
        return name;
    }

    public String getConfig() {
        return config;
    }

    public List<ProcessorDefinition> getOnFailure() {
        return onFailure;
    }

    @JsonSetter
    private void setConfig(Map config) {
        this.config = JsonUtils.toJsonString(config);
    }
}
