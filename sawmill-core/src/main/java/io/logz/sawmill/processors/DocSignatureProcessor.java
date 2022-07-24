package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

@ProcessorProvider(type = "docSignature", factory = DocSignatureProcessor.Factory.class)
public class DocSignatureProcessor implements Processor {
    private final SignatureMode signatureMode;
    private final Set<String> includeValueFields;
    private final String DOC_SIGNATURE_FIELD = "logzio_doc_signature";
    public DocSignatureProcessor(SignatureMode signatureMode, Set<String> includeValueFields) {
        this.signatureMode = signatureMode;
        this.includeValueFields = includeValueFields;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        String signature;
        try {
            signature = createSignature(doc);
        } catch (Exception e) {
            return ProcessResult.failure(
                    "failed to create signature, SignatureMode: " + signatureMode,
                    new ProcessorExecutionException(DocSignatureProcessor.class.getSimpleName(), e));
        }

        if(signature.isEmpty())
            return ProcessResult.failure("signature is empty, SignatureMode: " + signatureMode);

        addSignatureField(doc, signature);
        return ProcessResult.success();
    }

    private String createSignature(Doc doc) throws InterruptedException {
        switch(signatureMode) {
            case FIELDS_VALUES:
                return getFieldsValuesString(doc);
            case FIELDS_NAMES:
                return getFieldsNamesString(doc);
            case HYBRID:
                return getFieldsValuesString(doc) + getFieldsNamesString(doc);
            default: return "";
        }
    }

    private void addSignatureField(Doc doc, String signature) {
        doc.addField(DOC_SIGNATURE_FIELD, signature.hashCode());
    }

    private String getFieldsNamesString(Doc doc) throws InterruptedException {
        Set<String> fieldsNames = extractFieldsNames(doc);
        return fieldsNames.isEmpty() ? "" : JsonUtils.toJsonString(fieldsNames);
    }

    private String getFieldsValuesString(Doc doc) {
        List<String> values = includeValueFields.stream().collect(Collectors.toList())
                .stream()
                .filter(doc::hasField)
                .map(doc::getField)
                .map(Object::toString)
                .collect(Collectors.toList());
        return values.isEmpty() ? "" : JsonUtils.toJsonString(values);
    }

    private Set<String> extractFieldsNames(Doc doc) throws InterruptedException {
        Map<String, Object> source = doc.getSource();
        Set<String> fields = new HashSet<>();
        extractFieldsNames(source, null, fields);
        return fields;
    }

    public void extractFieldsNames(Object object, String key, Set<String> fields) throws InterruptedException {
        if (Thread.currentThread().isInterrupted()) throw new InterruptedException();
        if(object instanceof Map) {
            Map<String, Object> map = (Map) object;
            for(Map.Entry<String, Object> entry : map.entrySet()) {
                extractFieldsNames(entry.getValue(),
                        key != null ?
                                new StringBuilder(key).append('.').append(entry.getKey()).toString() : entry.getKey(),
                        fields);
            }
        } else if(isListOfMaps(object)) {
            List<Map<String, Object>> listOfMaps = (List<Map<String, Object>>) object;
            for(Map<String, Object> map : listOfMaps) {
                extractFieldsNames(map, key, fields);
            }
        } else {
            fields.add(key);
        }
    }

    private boolean isListOfMaps(Object object) {
        return object instanceof List && !((List) object).isEmpty() && ((List) object).get(0) instanceof Map;
    }

    public static class Factory implements Processor.Factory {
        public Factory() {}

        @Override
        public DocSignatureProcessor create(Map<String,Object> config) {
            DocSignatureProcessor.Configuration fieldsNamesSignatureConfig =
                    JsonUtils.fromJsonMap(DocSignatureProcessor.Configuration.class, config);
            return new DocSignatureProcessor(fieldsNamesSignatureConfig.getSignatureMode(),
                    fieldsNamesSignatureConfig.getIncludeValueFields());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private SignatureMode signatureMode = SignatureMode.FIELDS_NAMES;
        private Set<String> includeValueFields = new HashSet<>();
        public Configuration(SignatureMode signatureMode, Set<String> includeValueFields) {
            this.signatureMode = signatureMode;
            this.includeValueFields = includeValueFields;
        }
        public Configuration() {}

        public SignatureMode getSignatureMode() { return signatureMode; }
        public Set<String> getIncludeValueFields() { return includeValueFields; }
    }

    public enum SignatureMode {
        FIELDS_NAMES,
        FIELDS_VALUES,
        HYBRID
    }
}
