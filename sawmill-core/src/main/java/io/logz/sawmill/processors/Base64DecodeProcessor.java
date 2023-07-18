package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.Base64;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

@ProcessorProvider(type = "base64Decode", factory = Base64DecodeProcessor.Factory.class)
public class Base64DecodeProcessor implements Processor {

    private final Set<String> fields;
    private final boolean allowMissingFields;

    public Base64DecodeProcessor(Set<String> fields, boolean allowMissingFields) {
        this.fields = requireNonNull(fields);
        this.allowMissingFields = allowMissingFields;
    }

    @Override
    public ProcessResult process(Doc doc) {
        Set<String> missingFields = listMissingFields(doc);
        if(!validateMissingFields(missingFields))
            return ProcessResult.failure("some or all fields are missing from doc");

        fields.stream()
                .filter(field -> !missingFields.contains(field))
                .forEach(field -> {
                    String value = doc.getField(field);
                    doc.addField(field, new String(Base64.getDecoder().decode(value)));
                });
        return ProcessResult.success();
    }

    private boolean validateMissingFields(Set<String> missingFields) {
        return missingFields.isEmpty() ||
                (missingFields.size() < fields.size() && allowMissingFields);
    }

    private Set<String> listMissingFields(Doc doc) {
        return fields.stream()
                .filter(field -> !doc.hasField(field, String.class))
                .collect(Collectors.toSet());
    }

    public static class Factory implements Processor.Factory {
        public Factory() {}

        @Override
        public Base64DecodeProcessor create(Map<String,Object> config) {
            Base64DecodeProcessor.Configuration configuration =
                    JsonUtils.fromJsonMap(Base64DecodeProcessor.Configuration.class, config);
            validateConfiguration(configuration);
            return new Base64DecodeProcessor(configuration.getFields(), configuration.getAllowMissingFields());
        }

        private void validateConfiguration(Configuration configuration) {
            if(configuration.getFields().isEmpty())
                throw new ProcessorConfigurationException("fields can not be empty");
        }
    }

    public static class Configuration implements Processor.Configuration {
        private Set<String> fields;
        private boolean allowMissingFields;

        public Configuration() {}
        public Configuration(Set<String> fields, boolean allowMissingFields) {
            this.fields = fields;
            this.allowMissingFields = allowMissingFields;
        }

        public boolean getAllowMissingFields() { return allowMissingFields; }
        public Set<String> getFields() { return fields; }
    }
}
