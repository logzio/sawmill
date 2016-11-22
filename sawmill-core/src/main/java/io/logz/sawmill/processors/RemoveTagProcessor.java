package io.logz.sawmill.processors;

import io.logz.sawmill.AbstractProcessor;
import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.ProcessorFactoryRegistry;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

public class RemoveTagProcessor implements Processor {
    private static final String NAME = "removeTag";

    private final List<String> tags;
    private final List<Processor> onFailureProcessors;
    private final boolean ignoreFailure;

    public RemoveTagProcessor(List<String> tags, List<Processor> onFailureProcessors, boolean ignoreFailure) {
        this.tags = tags;
        this.onFailureProcessors = onFailureProcessors;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            doc.removeFromList("tags", tags);
        } catch (Exception e) {
            if (ignoreFailure) {
                return;
            }

            if (onFailureProcessors.isEmpty()) {
                throw new ProcessorExecutionException(getName(), String.format("failed to remove tags [%s]", tags), e);
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
            RemoveTagProcessor.Configuration removeTagConfig = JsonUtils.fromJsonString(RemoveTagProcessor.Configuration.class, config);

            List<Processor> onFailureProcessors = extractProcessors(removeTagConfig.getOnFailureProcessors(), processorFactoryRegistry);

            return new RemoveTagProcessor(removeTagConfig.getTags(), onFailureProcessors, removeTagConfig.isIgnoreFailure());
        }
    }

    public static class Configuration extends AbstractProcessor.Configuration {
        private List<String> tags;

        public Configuration() { }

        public Configuration(List<String> tags) {
            this.tags = tags;
        }

        public List<String> getTags() { return tags; }
    }
}
