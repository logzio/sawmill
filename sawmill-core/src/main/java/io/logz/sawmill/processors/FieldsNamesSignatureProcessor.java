package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

@ProcessorProvider(type = "fieldsNamesSignature", factory = FieldsNamesSignatureProcessor.Factory.class)
public class FieldsNamesSignatureProcessor implements Processor {

    private final boolean includeTypeFieldInSignature;
    private final String FIELDS_NAMES_SIGNATURE = "logzio_fields_signature";
    public FieldsNamesSignatureProcessor(boolean includeTypeFieldInSignature) {
        this.includeTypeFieldInSignature = includeTypeFieldInSignature;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        Set<String> fields;
        try {
            fields = extractFieldsNames(doc);
        } catch (Exception e) {
            return ProcessResult.failure(String.format("failed to add field %s to doc", FIELDS_NAMES_SIGNATURE));
        }
        addSignatureField(doc, fields);
        return ProcessResult.success();
    }

    private void addSignatureField(Doc doc, Set<String> fields) {
        doc.addField(FIELDS_NAMES_SIGNATURE, createSignature(fields, doc));
    }

    private int createSignature(Set<String> fields, Doc doc) {
        if(includeTypeFieldInSignature) {
            String type = doc.hasField("type") ? doc.getField("type") : "";
            return (type + JsonUtils.toJsonString(fields)).hashCode();
        }
        return JsonUtils.toJsonString(fields).hashCode();
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
                        key != null ? key + '.' + entry.getKey() : entry.getKey(),
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
        public FieldsNamesSignatureProcessor create(Map<String,Object> config) {
            FieldsNamesSignatureProcessor.Configuration fieldsNamesSignatureConfig =
                    JsonUtils.fromJsonMap(FieldsNamesSignatureProcessor.Configuration.class, config);
            return new FieldsNamesSignatureProcessor(fieldsNamesSignatureConfig.getIncludeTypeFieldInSignature());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private boolean includeTypeFieldInSignature;
        public Configuration(boolean includeTypeFieldInSignature) {
            this.includeTypeFieldInSignature = includeTypeFieldInSignature;
        }
        public Configuration() {}
        public boolean getIncludeTypeFieldInSignature() { return includeTypeFieldInSignature; }
    }
}
