package io.logz.sawmill.processors;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorParseException;
import io.logz.sawmill.utilities.JsonUtils;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@ProcessorProvider(type = ConvertFieldProcessor.TYPE, factory = ConvertFieldProcessor.Factory.class)
public class ConvertFieldProcessor implements Processor {
    public static final String TYPE = "convert";

    private final String path;
    private final FieldType fieldType;

    public ConvertFieldProcessor(String path, FieldType fieldType) {
        checkNotNull(fieldType);
        this.path = path;
        this.fieldType = fieldType;
    }

    @Override
    public String getType() {
        return TYPE;
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(path)) {
            return ProcessResult.failure("failed to convert field in path [%s], field is missing");
        }
        Object beforeCast = doc.getField(path);

        Object afterCast = fieldType.parse(beforeCast);

        if (afterCast == null) {
            return failureResult(beforeCast);
        }

        boolean succeeded = doc.removeField(path);
        if (succeeded) {
            doc.addField(path, afterCast);
            return ProcessResult.success();
        } else {
            return failureResult(beforeCast);
        }
    }

    private ProcessResult failureResult(Object beforeCastValue) {
        return ProcessResult.failure(String.format("failed to convert field in path [%s] to %s, value [%s]", path, fieldType, beforeCastValue));
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config) {
            ConvertFieldProcessor.Configuration convertFieldConfig = JsonUtils.fromJsonString(ConvertFieldProcessor.Configuration.class, config);

            if (convertFieldConfig.getType() == null) {
                throw new ProcessorParseException("failed to parse convert processor, could not resolve field type");
            }

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
        LONG {
            @Override
            public Object parse(Object value) {
                return Longs.tryParse(value.toString());
            }
        },
        DOUBLE {
            @Override
            public Object parse(Object value) {
                return Doubles.tryParse(value.toString());
            }
        },
        STRING {
            @Override
            public Object parse(Object value) {
                return value.toString();
            }
        },
        BOOLEAN {
            @Override
            public Object parse(Object value) {
                if (value.toString().matches("^(t|true|yes|y|1)$")) {
                    return true;
                } else if (value.toString().matches("^(f|false|no|n|0)$")) {
                    return false;
                } else {
                    return null;
                }
            }
        };

        @Override
        public String toString() {
            return this.name().toLowerCase();
        }

        public abstract Object parse(Object value);
    }
}
