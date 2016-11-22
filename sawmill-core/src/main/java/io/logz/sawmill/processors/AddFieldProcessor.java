package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

public class AddFieldProcessor implements Processor {
    private static final String NAME = "addField";

    private final String path;
    private final Object value;

    public AddFieldProcessor(String path, Object value) {
        this.path = path;
        this.value = value;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        doc.addField(path, value);
    }

    @ProcessorProvider(name = NAME)
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            AddFieldProcessor.Configuration addFieldConfig = JsonUtils.fromJsonString(AddFieldProcessor.Configuration.class, config);

            return new AddFieldProcessor(addFieldConfig.getPath(), addFieldConfig.getValue());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private Object value;

        public Configuration() { }

        public Configuration(String path, Object value) {
            this.path = path;
            this.value = value;
        }

        public String getPath() { return path; }

        public Object getValue() { return value; }
    }
}
