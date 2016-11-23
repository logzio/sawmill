package io.logz.sawmill.processors;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
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
        Object afterCast = null;
        Object beforeCast = doc.getField(path);
        switch (fieldType) {
            case LONG: {
                afterCast = Longs.tryParse(beforeCast.toString());
                break;
            }
            case DOUBLE: {
                afterCast = Doubles.tryParse(beforeCast.toString());
                break;
            }
            case BOOLEAN: {
                if (beforeCast.toString().matches("^(t|true|yes|y|1)$")) {
                    afterCast = true;
                } else if (beforeCast.toString().matches("^(f|false|no|n|0)$")) {
                    afterCast = false;
                }
                break;
            }
            case STRING: {
                afterCast = beforeCast.toString();
                break;
            }
        }

        if (afterCast == null) {
            return failureResult(beforeCast);
        }

        boolean succeeded = doc.removeField(path);
        if (succeeded) {
            doc.addField(path, afterCast);
            return new ProcessResult(true);
        } else {
            return failureResult(beforeCast);
        }
    }

    private ProcessResult failureResult(Object beforeCastValue) {
        return new ProcessResult(false, String.format("failed to convert field in path [%s] to %s, unknown value [%s]", path, fieldType, beforeCastValue));
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
