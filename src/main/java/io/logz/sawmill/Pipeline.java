package io.logz.sawmill;

import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Pipeline {

    private final String id;
    private final String description;
    private final List<Process> processes;

    public Pipeline(String id, String description, List<Process> processes) {
        checkArgument(id.isEmpty(), "id cannot be empty");
        checkArgument(CollectionUtils.isEmpty(processes), "processes cannot be empty");
        this.id = id;
        this.description = description;
        this.processes = processes;
    }

    private void checkArgument(boolean notValid, String errorMsg) {
        if (notValid) throw new IllegalArgumentException(errorMsg);
    }

    public String getId() { return id; }

    public String getDescription() { return description; }

    public List<Process> getProcesses() { return processes; }

    public static final class Factory {

        private final ProcessFactoryRegistry processFactoryRegistry;

        public Factory() {
            this.processFactoryRegistry = new ProcessFactoryRegistry();
        }

        public Factory(ProcessFactoryRegistry processFactoryRegistry) {
            this.processFactoryRegistry = processFactoryRegistry;
        }

        public Pipeline create(Configuration config) {
            List<Process> processes = new ArrayList<>();

            config.getProcesses().forEach(processorDefinition -> {
                Process.Factory factory = processFactoryRegistry.get(processorDefinition.getName());
                processes.add(factory.create(JsonUtils.toJsonString(processorDefinition.getConfig())));
            });

            return new Pipeline(config.getId(),
                    config.getDescription(),
                    processes);
        }
    }

    public static class Configuration {
        private String id;
        private String description;
        private List<ProcessDefinition> processes;

        public Configuration() { }

        public String getId() {
            return id;
        }

        public String getDescription() {
            return description;
        }

        public List<ProcessDefinition> getProcesses() {
            return processes;
        }
    }

    public static class ProcessDefinition {
        private String name;
        private Map<String,Object> config;

        public ProcessDefinition() { }

        public String getName() {
            return name;
        }

        public Map<String,Object> getConfig() {
            return config;
        }
    }
}
