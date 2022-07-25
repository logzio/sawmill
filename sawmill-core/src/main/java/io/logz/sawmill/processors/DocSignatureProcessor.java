package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.exceptions.ProcessorExecutionException;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.CollectionUtils;

import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@ProcessorProvider(type = "docSignature", factory = DocSignatureProcessor.Factory.class)
public class DocSignatureProcessor implements Processor {
    private final SignatureMode signatureMode;
    private final Set<String> includeValueFields;
    private final String signatureFieldName;
    public DocSignatureProcessor(SignatureMode signatureMode, String signatureFieldName, Set<String> includeValueFields) {
        this.signatureMode = signatureMode;
        this.signatureFieldName = signatureFieldName;
        this.includeValueFields = includeValueFields;
    }

    @Override
    public ProcessResult process(Doc doc) throws InterruptedException {
        Collection<String> signatureCollection;
        try {
            signatureCollection = createSignatureCollection(doc);
        } catch (Exception e) {
            return ProcessResult.failure(
                    "failed to create signature, SignatureMode: " + signatureMode,
                    new ProcessorExecutionException(DocSignatureProcessor.class.getSimpleName(), e));
        }

        if(signatureCollection.isEmpty()) {
            if(signatureMode.equals(SignatureMode.FIELDS_NAMES) || signatureMode.equals(SignatureMode.HYBRID)) {
                return ProcessResult.failure("failed to extract fields names, SignatureMode: " + signatureMode);
            }
            return ProcessResult.failure("failed to add signature field, signature collection is empty");
        }

        addSignatureField(doc, signatureCollection);
        return ProcessResult.success();
    }

    private Collection<String> createSignatureCollection(Doc doc) throws InterruptedException {
        switch(signatureMode) {
            case FIELDS_VALUES:
                return getFieldsValues(doc);
            case FIELDS_NAMES:
                return extractFieldsNames(doc);
            case HYBRID:
                return Stream.concat(getFieldsValues(doc).stream(), extractFieldsNames(doc)
                        .stream()).collect(Collectors.toList());
            default: return Collections.emptyList();
        }
    }

    private void addSignatureField(Doc doc, Collection<String> signatureCollection) {
        doc.addField(signatureFieldName, hash(signatureCollection));
    }

    private List<String> getFieldsValues(Doc doc) {
        return includeValueFields.stream()
                .filter(doc::hasField)
                .map(doc::getField)
                .map(Object::toString)
                .collect(Collectors.toList());
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

    public int hash(Collection<String> collection) {
        int h = 0;
        Iterator<String> i = collection.iterator();
        while(i.hasNext()) {
            String str = i.next();
            if(str != null) {
                h += str.hashCode();
            }
        }
        return h;
    }

    public static class Factory implements Processor.Factory {
        public Factory() {}

        @Override
        public DocSignatureProcessor create(Map<String,Object> config) {
            DocSignatureProcessor.Configuration docSignatureConfig =
                    JsonUtils.fromJsonMap(DocSignatureProcessor.Configuration.class, config);

            validateConfiguration(docSignatureConfig);

            return new DocSignatureProcessor(docSignatureConfig.getSignatureMode(),
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