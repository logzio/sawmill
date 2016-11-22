package io.logz.sawmill;

import java.util.List;
import java.util.Map;

public interface Processor {
    void process(Doc doc);

    String getName();

    interface Factory {
        Processor create(String config, ProcessorFactoryRegistry processorFactoryRegistry);
    }

    interface Configuration {
        List<ProcessorDefinition> getOnFailureProcessors();

        Boolean isIgnoreFailure();
    }

    class ProcessorDefinition {
        private String name;
        private Map<String,Object> config;

        public ProcessorDefinition() { }

        public String getName() {
            return name;
        }

        public Map<String,Object> getConfig() {
            return config;
        }
    }
}
