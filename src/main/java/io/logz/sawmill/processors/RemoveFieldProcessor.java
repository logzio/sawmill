package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RemoveFieldProcessor implements Processor {
    private static final String NAME = "removeField";
    private static final Logger logger = LoggerFactory.getLogger(RemoveFieldProcessor.class);

    private final String path;

    public RemoveFieldProcessor(String path) {
        this.path = path;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            doc.removeField(path);
        } catch (IllegalStateException e) {
            logger.trace("failed to remove field in path [{}]", path, e);
        }
    }

    @ProcessorProvider(name = NAME)
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            RemoveFieldProcessor.Configuration removeFieldConfig = JsonUtils.fromJsonString(RemoveFieldProcessor.Configuration.class, config);

            return new RemoveFieldProcessor(removeFieldConfig.getPath());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;

        public Configuration() { }

        public Configuration(String path, boolean ignoreMissing) {
            this.path = path;
        }

        public String getPath() { return path; }
    }
}
