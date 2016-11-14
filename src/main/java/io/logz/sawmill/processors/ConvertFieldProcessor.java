package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConvertFieldProcessor implements Processor {
    private static final String NAME = "convertField";
    private static final Logger logger = LoggerFactory.getLogger(ConvertFieldProcessor.class);


    private final String path;
    private final FieldType type;

    public ConvertFieldProcessor(String path, FieldType type) {
        this.path = path;
        this.type = type;
    }

    public FieldType getType() {
        return type;
    }

    @Override
    public String getName() { return NAME; }

    @Override
    public void process(Doc doc) {
        if (type == null) {
            logger.trace("failed to convert field in path [{}], type is null", path);
            return;
        }

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
                        logger.trace("failed to convert field in path [{}] to Boolean, unknown value [{}]", path, beforeCast);
                        return;
                    }
                    break;
                }
                case STRING: {
                    afterCast = beforeCast.toString();
                    break;
                } default: {
                    logger.trace("failed to convert field in path [{}], unknown field type", path);
                    return;
                }
            }

            doc.removeField(path);
            doc.addField(path, afterCast);
        } catch (Exception e){
            logger.trace("failed to convert field in path [{}] to type [{}]", path, type, e);
        }

    }

    @ProcessorProvider(name = NAME)
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
