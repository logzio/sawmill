package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.FieldType;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "convert", factory = ConvertFieldProcessor.Factory.class)
public class ConvertFieldProcessor implements Processor {

    private final List<String> paths;
    private final FieldType fieldType;

    public ConvertFieldProcessor(List<String> paths, FieldType fieldType) {
        this.paths = requireNonNull(paths, "paths cannot be null");
        this.fieldType = requireNonNull(fieldType, "field type cannot be null");
    }

    public FieldType getFieldType() {
        return fieldType;
    }

    @Override
    public ProcessResult process(Doc doc) {
        List<String> errorMessages = new ArrayList<>();
        for (String path : paths) {
            if (!doc.hasField(path)) {
                errorMessages.add(String.format("failed to convert field in path [%s], field is missing.", path));
                continue;
            }
            Object beforeCast = doc.getField(path);

            Object afterCast = fieldType.convertFrom(beforeCast);

            if (afterCast == null) {
                errorMessages.add(String.format("failed to convert field in path [%s] to %s, value [%s].", path, fieldType, beforeCast));
                continue;
            }

            boolean succeeded = doc.removeField(path);
            if (succeeded) {
                doc.addField(path, afterCast);
            }
            else {
                errorMessages.add(String.format("failed to convert field in path [%s] to %s, value [%s].", path, fieldType, beforeCast));
            }
        }
        if (errorMessages.isEmpty()) {
            return ProcessResult.success();
        }
        String allErrorMessages = errorMessages.stream().collect(Collectors.joining("\n"));
        return ProcessResult.failure(allErrorMessages);
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public ConvertFieldProcessor create(Map<String,Object> config) {

            ConvertFieldProcessor.Configuration convertFieldConfig = JsonUtils.fromJsonMap(ConvertFieldProcessor.Configuration.class, config);

            if (convertFieldConfig.getPath() == null && convertFieldConfig.getPaths() == null) {
                throw new ProcessorConfigurationException("failed to parse convert processor config, could not resolve field path/s");
            }

            if (convertFieldConfig.getPath() != null && convertFieldConfig.getPaths() != null) {
                throw new ProcessorConfigurationException("failed to parse convert processor config, both field path and field paths are defined when only 1 allowed");
            }

            if (convertFieldConfig.getType() == null) {
                throw new ProcessorConfigurationException("failed to parse convert processor config, could not resolve field type");
            }

            List<String> paths = convertFieldConfig.getPath() == null ? convertFieldConfig.getPaths() : Collections.singletonList(convertFieldConfig.getPath());

            return new ConvertFieldProcessor(paths, convertFieldConfig.getType());
        }
    }

    public static class Configuration implements Processor.Configuration {

        private String path;
        private List<String> paths;
        private FieldType type;

        public String getPath() { return path; }

        public FieldType getType() { return type; }

        public List<String> getPaths() {
            return paths;
        }
    }
}
