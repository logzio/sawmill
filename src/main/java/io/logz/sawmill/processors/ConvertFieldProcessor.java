package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.codehaus.jackson.annotate.JsonProperty;

public class ConvertFieldProcessor implements Processor {
    public static final String NAME = "convertField";

    private final String path;
    private final FieldType type;

    public ConvertFieldProcessor(String path, FieldType type) {
        this.path = path;
        this.type = type;
    }

    public String getPath() {
        return path;
    }

    public FieldType getType() {
        return type;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            doc.convertField(path, Class.forName(type.getClassName()));
        } catch (ClassNotFoundException e) {

        }
    }

    @ProcessorProvider(name = "convertField")
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            ConvertFieldProcessor.Configuration convertFieldConfig = JsonUtils.fromJsonString(ConvertFieldProcessor.Configuration.class, config);

            return new ConvertFieldProcessor(convertFieldConfig.getPath(), convertFieldConfig.isType());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String path;
        private FieldType type;

        public Configuration() { }

        public Configuration(String path, FieldType type) {
            this.path = path;
            this.type = type;
        }

        public String getPath() { return path; }

        public FieldType isType() { return type; }
    }

    public enum FieldType {
        INTEGER("java.lang.Integer"),
        FLOAT("java.lang.Float"),
        STRING("java.lang.String"),
        BOOLEAN("java.lang.Boolean");

        private final String className;

        FieldType(String className) {
            this.className = className;
        }

        public String getClassName() {
            return className;
        }
    }
}
