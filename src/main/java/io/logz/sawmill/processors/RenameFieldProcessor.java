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

public class RenameFieldProcessor implements Processor {
    public static final String NAME = "renameField";
    private static final Logger logger = LoggerFactory.getLogger(RenameFieldProcessor.class);

    private final String from;
    private final String to;
    private final List<Processor> onFailureProcessors;
    private final boolean ignoreFailure;

    public RenameFieldProcessor(String from, String to, List<Processor> onFailureProcessors, boolean ignoreFailure) {
        this.from = from;
        this.to = to;
        this.onFailureProcessors = onFailureProcessors;
        this.ignoreFailure = ignoreFailure;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            Object fieldValue = doc.getField(from);
            doc.removeField(from);
            doc.addField(to, fieldValue);
        } catch (Exception e) {
            if (ignoreFailure) {
                return;
            }

            if (onFailureProcessors.isEmpty()) {
                throw new ProcessorExecutionException(getName(), String.format("failed to rename field [%s] to [%s]", from, to), e);
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
            RenameFieldProcessor.Configuration renameFieldConfig = JsonUtils.fromJsonString(RenameFieldProcessor.Configuration.class, config);

            List<Processor> onFailureProcessors = extractProcessors(renameFieldConfig.getOnFailureProcessors(), processorFactoryRegistry);

            return new RenameFieldProcessor(renameFieldConfig.getFrom(), renameFieldConfig.getTo(), onFailureProcessors, renameFieldConfig.isIgnoreFailure());
        }
    }

    public static class Configuration extends AbstractProcessor.Configuration {
        private String from;
        private String to;

        public Configuration() { }

        public Configuration(String from, String to) {
            this.from = from;
            this.to = to;
        }

        public String getFrom() { return from; }

        public String getTo() { return to; }
    }
}
