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
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;

public class ConvertFieldProcessor implements Processor {
    private static final String NAME = "convertField";

    private final String path;
    private final FieldType type;
    private final List<Processor> onFailureProcessors;
    private final boolean ignoreFailure;

    public ConvertFieldProcessor(String path, FieldType type, List<Processor> onFailureProcessors, boolean ignoreFailure) {
        checkState(type != null, "convert type cannot be empty");
        this.path = path;
        this.type = type;
        this.onFailureProcessors = onFailureProcessors;
        this.ignoreFailure = ignoreFailure;
    }

    public FieldType getType() {
        return type;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        try {
            Object afterCast;
            Object beforeCast = doc.getField(path);
            switch (type) {
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
                        handleFailure(doc, String.format("failed to convert field in path [%s] to Boolean, unknown value [%s]", path, beforeCast), Optional.empty());
                        return;
                    }
                    break;
                }
                case STRING: {
                    afterCast = beforeCast.toString();
                    break;
                } default: {
                    handleFailure(doc, String.format("failed to convert field in path [%s], unknown field type", path), Optional.empty());
                    return;
                }
            }

            doc.removeField(path);
            doc.addField(path, afterCast);
        } catch (Exception e) {
            handleFailure(doc, String.format("failed to convert field in path [%s] to type [%s]", path, type), Optional.of(e));
        }

    }

    private void handleFailure(Doc doc, String errorMsg, Optional<Exception> e) {
        if (ignoreFailure) {
            return;
        }

        if (onFailureProcessors.isEmpty()) {
            if (e.isPresent()) {
                throw new ProcessorExecutionException(getName(), errorMsg, e.get());
            } else {
                throw new ProcessorExecutionException(getName(), errorMsg);
            }
        } else {
            for (Processor processor : onFailureProcessors) {
                processor.process(doc);
            }
        }
    }

    @ProcessorProvider(name = NAME)
    public static class Factory extends AbstractProcessor.Factory {
        public Factory() {
        }

        @Override
        public Processor create(String config, ProcessorFactoryRegistry processorFactoryRegistry) {
            ConvertFieldProcessor.Configuration convertFieldConfig = JsonUtils.fromJsonString(ConvertFieldProcessor.Configuration.class, config);

            List<Processor> onFailureProcessors = extractProcessors(convertFieldConfig.getOnFailureProcessors(), processorFactoryRegistry);

            return new ConvertFieldProcessor(convertFieldConfig.getPath(), convertFieldConfig.getType(), onFailureProcessors, convertFieldConfig.isIgnoreFailure());
        }
    }

    public static class Configuration extends AbstractProcessor.Configuration {
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
