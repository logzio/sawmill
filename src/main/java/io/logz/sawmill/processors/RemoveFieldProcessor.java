package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

public class RemoveFieldProcessor implements Processor {
    public static final String NAME = "removeField";

    private final String path;

    public RemoveFieldProcessor(String path) {
        this.path = path;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        doc.removeField(path);
    }

    @ProcessorProvider(name = "removeField")
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            RemoveFieldProcessor.Configuration addFieldConfig = JsonUtils.fromJsonString(RemoveFieldProcessor.Configuration.class, config);

            return new RemoveFieldProcessor(addFieldConfig.getPath());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;

        public Configuration() { }

        public Configuration(String path) {
            this.path = path;
        }

        public String getPath() { return path; }
    }
}
