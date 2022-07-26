package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ProcessorProvider(type = "addSignature", factory = AddSignatureProcessor.Factory.class)
public class AddSignatureProcessor implements Processor {
    private final SignatureMode signatureMode;
    private final Set<String> includeValueFields;
    private final String signatureFieldName;
    public AddSignatureProcessor(SignatureMode signatureMode, String signatureFieldName, Set<String> includeValueFields) {
        this.signatureMode = signatureMode;
        this.signatureFieldName = signatureFieldName;
        this.includeValueFields = includeValueFields;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        int signature;
        try {
            signature = createSignature(doc);
        } catch (Exception e) {
            return ProcessResult.failure(
                    "failed to create signature, SignatureMode: " + signatureMode,
                    new ProcessorExecutionException(AddSignatureProcessor.class.getSimpleName(), e));
        }

        if(signature == 0) {
            if(signatureMode.equals(SignatureMode.FIELDS_NAMES) || signatureMode.equals(SignatureMode.HYBRID)) {
                return ProcessResult.failure("failed to extract fields names, SignatureMode: " + signatureMode);
            }
            return ProcessResult.failure("failed to add signature field, signature collection is empty");
        }

        addSignatureField(doc, signature);
        return ProcessResult.success();
    }

    private int createSignature(Doc doc) throws InterruptedException {
        switch(signatureMode) {
            case FIELDS_NAMES:
                return hashIfNotEmpty(extractFieldsNames(doc));
            case FIELDS_VALUES:
                return hashIfNotEmpty(getFieldsValues(doc));
            case HYBRID:
                return hashIfNotEmpty(extractFieldsNames(doc)) + hashIfNotEmpty(getFieldsValues(doc));
            default: return 0;
        }
    }

    private int hashIfNotEmpty(Set<String> set) {
        return set.isEmpty() ? 0 : set.hashCode();
    }

    private void addSignatureField(Doc doc, int signature) {
        doc.addField(signatureFieldName, signature);
    }

    private Set<String> getFieldsValues(Doc doc) {
        return includeValueFields.stream()
                .filter(doc::hasField)
                .map(fieldName -> {
                    String value = doc.getField(fieldName);
                    return "value_" + fieldName + "_" + value;
                }).collect(Collectors.toSet());
    }

    private Set<String> extractFieldsNames(Doc doc) throws InterruptedException {
        Map<String, Object> source = doc.getSource();
        Set<String> fields = new HashSet<>();
        extractFieldsNames(source, null, fields);
        return fields;
    }

    public void extractFieldsNames(Object object, String parentKey, Set<String> fields) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
        if(object instanceof Map) {
            Map<String, Object> map = (Map) object;
            for(Map.Entry<String, Object> entry : map.entrySet()) {
                String fieldPath = parentKey != null ?
                        new StringBuilder(parentKey).append('.').append(entry.getKey()).toString() : entry.getKey();
                extractFieldsNames(entry.getValue(), fieldPath, fields);
            }
        } else if(isListOfMaps(object)) {
            List<Map<String, Object>> listOfMaps = (List<Map<String, Object>>) object;
            for(Map<String, Object> map : listOfMaps) {
                extractFieldsNames(map, parentKey, fields);
            }
        } else {
            fields.add(parentKey);
        }
    }

    private boolean isListOfMaps(Object object) {
        return object instanceof List && !((List) object).isEmpty() && ((List) object).get(0) instanceof Map;
    }

    public static class Factory implements Processor.Factory {
        public Factory() {}

        @Override
        public AddSignatureProcessor create(Map<String,Object> config) {
            AddSignatureProcessor.Configuration docSignatureConfig =
                    JsonUtils.fromJsonMap(AddSignatureProcessor.Configuration.class, config);

            validateConfiguration(docSignatureConfig);

            return new AddSignatureProcessor(docSignatureConfig.getSignatureMode(),
                    docSignatureConfig.getSignatureFieldName(),
                    docSignatureConfig.getIncludeValueFields());
        }

        private void validateConfiguration(Configuration docSignatureConfig) {
            if(docSignatureConfig.signatureFieldName.isEmpty())
                throw new ProcessorConfigurationException("signatureFieldName can not be empty");

            if((docSignatureConfig.signatureMode == SignatureMode.FIELDS_VALUES ||
                docSignatureConfig.signatureMode == SignatureMode.HYBRID)
                    && CollectionUtils.isEmpty(docSignatureConfig.includeValueFields)) {
                throw new ProcessorConfigurationException("includeValueFields can not be empty");
            }
        }
    }

    public static class Configuration implements Processor.Configuration {
        private SignatureMode signatureMode = SignatureMode.FIELDS_NAMES;
        private String signatureFieldName;
        private Set<String> includeValueFields = new HashSet<>();
        public Configuration(SignatureMode signatureMode, String signatureFieldName, Set<String> includeValueFields) {
            this.signatureMode = signatureMode;
            this.signatureFieldName = signatureFieldName;
            this.includeValueFields = includeValueFields;
        }
        public Configuration() {}

        public SignatureMode getSignatureMode() { return signatureMode; }
        public String getSignatureFieldName() { return signatureFieldName; }
        public Set<String> getIncludeValueFields() { return includeValueFields; }
    }

    public enum SignatureMode {
        FIELDS_NAMES,
        FIELDS_VALUES,
        HYBRID
    }
}
