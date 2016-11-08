package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

public class RemoveFieldProcessor implements Processor {
    public static final String NAME = "removeField";

    private final String path;
    private final boolean ignoreMissing;

    public RemoveFieldProcessor(String path, boolean ignoreMissing) {
        this.path = path;
        this.ignoreMissing = ignoreMissing;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            doc.removeField(path);
        } catch (IllegalStateException e) {
            if (!ignoreMissing) throw e;
        }
    }

    @ProcessorProvider(name = "removeField")
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            RemoveFieldProcessor.Configuration removeFieldConfig = JsonUtils.fromJsonString(RemoveFieldProcessor.Configuration.class, config);

            return new RemoveFieldProcessor(removeFieldConfig.getPath(), removeFieldConfig.isIgnoreMissing());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private boolean ignoreMissing;

        public Configuration() { }

        public Configuration(String path, boolean ignoreMissing) {
            this.path = path;
            this.ignoreMissing = ignoreMissing;
        }

        public String getPath() { return path; }

        public boolean isIgnoreMissing() { return ignoreMissing; }
    }
}
