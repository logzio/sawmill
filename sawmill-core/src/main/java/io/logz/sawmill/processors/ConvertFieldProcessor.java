package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import static com.google.common.base.Preconditions.checkState;

public class ConvertFieldProcessor implements Processor {
    private static final String TYPE = "convertField";

    private final String path;
    private final FieldType fieldType;

    public ConvertFieldProcessor(String path, FieldType fieldType) {
        checkState(fieldType != null, "convert fieldType cannot be empty");
        this.path = path;
        this.fieldType = fieldType;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    @Override
    public String getType() { return TYPE; }

    @Override
    public ProcessResult process(Doc doc) {
        Object afterCast;
        Object beforeCast = doc.getField(path);
        switch (fieldType) {
            case LONG: {
                afterCast = Long.parseLong(beforeCast.toString());
                break;
            }
            case DOUBLE: {
                afterCast = Double.parseDouble(beforeCast.toString());
                break;
            }
            case BOOLEAN: {
                if (beforeCast.toString().matches("^(t|true|yes|y|1)$")) {
                    afterCast = true;
                } else if (beforeCast.toString().matches("^(f|false|no|n|0)$")) {
                    afterCast = false;
                } else {
                    return failureResult(String.format("failed to convert field in path [%s] to Boolean, unknown value [%s]", path, beforeCast));
                }
                break;
            }
            case STRING: {
                afterCast = beforeCast.toString();
                break;
            } default: {
                return failureResult(String.format("failed to convert field in path [%s], unknown field fieldType", path));
            }
        }

        boolean succeeded = doc.removeField(path);
        if (succeeded) {
            doc.addField(path, afterCast);
            return new ProcessResult(true);
        } else {
            return failureResult(String.format("failed to convert field in path [%s] to fieldType [%s]", path, fieldType));
        }
    }

    private ProcessResult failureResult(String errorMsg) {
        return new ProcessResult(false, errorMsg);
    }

    @ProcessorProvider(name = TYPE)
    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            ConvertFieldProcessor.Configuration convertFieldConfig = JsonUtils.fromJsonString(ConvertFieldProcessor.Configuration.class, config);

            return new ConvertFieldProcessor(convertFieldConfig.getPath(), convertFieldConfig.getType());
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

        public FieldType getType() { return type; }
    }

    public enum FieldType {
        LONG,
        DOUBLE,
        STRING,
        BOOLEAN;

        @Override
        public String toString() {
            return this.name().toLowerCase();
        }
    }
}
