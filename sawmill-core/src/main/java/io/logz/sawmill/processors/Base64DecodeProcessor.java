package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Base64;
import java.util.Map;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "base64Decode", factory = Base64DecodeProcessor.Factory.class)
public class Base64DecodeProcessor implements Processor {

    private final String sourceField;
    private final String targetField;

    public Base64DecodeProcessor(String sourceField, String targetField) {
        this.sourceField = requireNonNull(sourceField);
        this.targetField = requireNonNull(targetField);
    }

    @Override
    public ProcessResult process(Doc doc) {
        if(!doc.hasField(sourceField, String.class))
            return ProcessResult.failure("field is missing from doc");

        String value = doc.getField(sourceField);
        String decodedValue = new String(Base64.getDecoder().decode(value));
        doc.addField(targetField, decodedValue);
        return ProcessResult.success();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {}

        @Override
        public Base64DecodeProcessor create(Map<String,Object> config) {
            Base64DecodeProcessor.Configuration configuration =
                    JsonUtils.fromJsonMap(Base64DecodeProcessor.Configuration.class, config);
            validateConfiguration(configuration);
            return new Base64DecodeProcessor(configuration.getSourceField(), configuration.getTargetField());
        }

        private void validateConfiguration(Configuration configuration) {
            if(configuration.getSourceField() == null || configuration.getSourceField().isEmpty()
                || configuration.getTargetField() == null || configuration.getTargetField().isEmpty())
                throw new ProcessorConfigurationException("sourceField, targetField can not be null or empty");
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String sourceField;
        private String targetField;

        public Configuration() {}
        public Configuration(String sourceField, String targetField) {
            this.sourceField = sourceField;
            this.targetField = targetField;
        }

        public String getSourceField() { return sourceField; }
        public String getTargetField() { return targetField; }
    }
}
