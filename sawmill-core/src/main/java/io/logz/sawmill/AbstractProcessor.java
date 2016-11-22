package io.logz.sawmill;

import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

public abstract class AbstractProcessor implements Processor {
    public static abstract class Factory implements Processor.Factory {
        public List<Processor> extractProcessors(List<ProcessorDefinition> processorDefinitions, ProcessorFactoryRegistry processorFactoryRegistry) {
            List<Processor> processors = new ArrayList<>();
            if (!CollectionUtils.isEmpty(processorDefinitions)) {
                processorDefinitions.forEach(processorDefinition -> {
                    Processor.Factory factory = processorFactoryRegistry.get(processorDefinition.getName());
                    processors.add(factory.create(JsonUtils.toJsonString(processorDefinition.getConfig()), processorFactoryRegistry));
                });
            }

            return processors;
        }
    }
    public static abstract class Configuration implements Processor.Configuration {
        private List<ProcessorDefinition> onFailureProcessors;
        private Boolean ignoreFailure;

        @Override
        public List<ProcessorDefinition> getOnFailureProcessors() {
            return onFailureProcessors;
        }

        @Override
        public Boolean isIgnoreFailure() {
            return (ignoreFailure != null ? ignoreFailure : false);
        }
    }


}
