package io.logz.sawmill.processors;

import com.google.common.primitives.Doubles;
import com.google.common.primitives.Longs;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorParseException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Map;

import static com.google.common.base.Preconditions.checkNotNull;

@ProcessorProvider(type = "convert", factory = ConvertFieldProcessor.Factory.class)
public class ConvertFieldProcessor implements Processor {
    private final String path;
    private final FieldType fieldType;

    public ConvertFieldProcessor(String path, FieldType fieldType) {
        this.path = checkNotNull(path, "path cannot be null");
        this.fieldType = checkNotNull(fieldType, "field type cannot be null");
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

        Object afterCast = fieldType.convertFrom(beforeCast);

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
        public ConvertFieldProcessor create(Map<String,Object> config) {
            ConvertFieldProcessor.Configuration convertFieldConfig = JsonUtils.fromJsonMap(ConvertFieldProcessor.Configuration.class, config);

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
            public Object convertFrom(Object value) {
                return Longs.tryParse(value.toString());
            }
        },
        DOUBLE {
            @Override
            public Object convertFrom(Object value) {
                return Doubles.tryParse(value.toString());
            }
        },
        STRING {
            @Override
            public Object convertFrom(Object value) {
                return value.toString();
            }
        },
        BOOLEAN {
            @Override
            public Object convertFrom(Object value) {
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

        public abstract Object convertFrom(Object value);
    }
}
