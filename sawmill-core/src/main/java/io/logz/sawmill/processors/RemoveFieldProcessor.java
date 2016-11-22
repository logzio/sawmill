package io.logz.sawmill.processors;

import io.logz.sawmill.AbstractProcessor;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.List;

public class RemoveFieldProcessor implements Processor {
    private static final String NAME = "removeField";

    private final String path;
    private final List<Processor> onFailureProcessors;
    private final boolean ignoreFailure;

    public RemoveFieldProcessor(String path, List<Processor> onFailureProcessors, boolean ignoreFailure) {
        this.path = path;
        this.onFailureProcessors = onFailureProcessors;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            doc.removeField(path);
        } catch (IllegalStateException e) {
            if (ignoreFailure) {
                return;
            }

            if (onFailureProcessors.isEmpty()) {
                throw new ProcessorExecutionException(getName(), String.format("failed to remove field in path [%s]", path), e);
            } else {
                for (Processor processor : onFailureProcessors) {
                    processor.process(doc);
                }
            }
        }
    }

    @ProcessorProvider(name = NAME)
    public static class Factory extends AbstractProcessor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config, ProcessorFactoryRegistry processorFactoryRegistry) {
            RemoveFieldProcessor.Configuration removeFieldConfig = JsonUtils.fromJsonString(RemoveFieldProcessor.Configuration.class, config);

            List<Processor> onFailureProcessors = extractProcessors(removeFieldConfig.getOnFailureProcessors(), processorFactoryRegistry);

            return new RemoveFieldProcessor(removeFieldConfig.getPath(), onFailureProcessors, removeFieldConfig.isIgnoreFailure());
        }
    }

    public static class Configuration extends AbstractProcessor.Configuration {
        private String path;

        public Configuration() { }

        public Configuration(String path) {
            this.path = path;
        }

        public String getPath() { return path; }
    }
}
