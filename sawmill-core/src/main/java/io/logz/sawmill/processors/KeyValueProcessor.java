package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "kv", factory = KeyValueProcessor.Factory.class)
public class KeyValueProcessor implements Processor {

    private final String field;
    private final String targetField;
    private final List<String> includeKeys;
    private final List<String> excludeKeys;
    private final String fieldSplit;
    private final String valueSplit;
    private final boolean allowDuplicateValues;
    private final boolean includeBrackets;
    private final String prefix;
    private final boolean recursive;
    private final String trim;
    private final String trimKey;

    public KeyValueProcessor(String field,
                             String targetField,
                             List<String> includeKeys,
                             List<String> excludeKeys,
                             String fieldSplit,
                             String valueSplit,
                             boolean allowDuplicateValues,
                             boolean includeBrackets,
                             String prefix,
                             boolean recursive,
                             String trim,
                             String trimKey) {
        this.field = field;
        this.targetField = targetField;
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
        this.fieldSplit = fieldSplit;
        this.valueSplit = valueSplit;
        this.allowDuplicateValues = allowDuplicateValues;
        this.includeBrackets = includeBrackets;
        this.prefix = prefix;
        this.recursive = recursive;
        this.trim = trim;
        this.trimKey = trimKey;
    }

    @Override
    public ProcessResult process(Doc doc) {
        if (!doc.hasField(field)) {
            return ProcessResult.failure(String.format("failed to process kv, couldn't find field [%s]", field));
        }

        String message = doc.getField(this.field);

        Map<String, Object> kvMap = (Map<String, Object>) getKeyValues(message);

        if (includeKeys != null) {
            kvMap.keySet().retainAll(includeKeys);
        } else if (excludeKeys != null) {
            kvMap.keySet().removeAll(excludeKeys);
        }

        if (targetField != null) {
            doc.addField(targetField, kvMap);
        } else {
            kvMap.forEach((key,value) -> {
                doc.addField(getKey(key), getValue(value));
            });
        }

        return ProcessResult.success();
    }

    private String getKey(String key) {
        return prefix + trim(key, trimKey);
    }

    private Object getValue(Object value) {
        if (value instanceof Map) {
            Map<String,Object> kvMap = new HashMap<>();

            Map<String,Object> map = (Map) value;
            map.forEach((innerKey, innerValue) -> {
                kvMap.put(getKey(innerKey), getValue(innerValue));
            });

            return kvMap;
        } else {
            return trim((String) value, trim);
        }
    }

    private Object getKeyValues(String message) {
        Map<String,Object> kvMap = new HashMap<>();

        String[] fields = includeBrackets ? message.split("[ ](?=[^\\]\\)>\\}]*?(?:\\[|\\(|<|\\{|$))") : message.split(fieldSplit);

        if (fields.length == 1) {
            return message;
        }

        for (String field : fields) {
            if (field.contains(valueSplit)) {
                String[] kv = field.split(valueSplit);
                String key = kv[0];
                Object value = recursive ? getKeyValues(kv[1]) : kv[1];

                if (allowDuplicateValues) {
                    kvMap.put(key, value);
                } else {
                    kvMap.computeIfPresent(key, (k, oldVal) -> {
                        if (oldVal instanceof List) {
                            return ((List) oldVal).add(value);
                        }
                        return Arrays.asList(oldVal, value);
                    });
                }
            }
        }
        return kvMap;
    }

    private String trim(String key, String trimChars) {
        if (trimChars != null) {
            StringUtils.strip(key, trimChars);
        }
        return key;
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public KeyValueProcessor create(Map<String,Object> config) {
            KeyValueProcessor.Configuration keyValueConfig = JsonUtils.fromJsonMap(KeyValueProcessor.Configuration.class, config);

            return new KeyValueProcessor(keyValueConfig.getField(),
                    keyValueConfig.getTargetField(),
                    keyValueConfig.getIncludeKeys(),
                    keyValueConfig.getExcludeKeys(),
                    keyValueConfig.getFieldSplit(),
                    keyValueConfig.getValueSplit(),
                    keyValueConfig.isAllowDuplicateValues(),
                    keyValueConfig.isIncludeBrackets(),
                    keyValueConfig.getPrefix(),
                    keyValueConfig.isRecursive(),
                    keyValueConfig.getTrim(),
                    keyValueConfig.getTrimKey());
        }
    }

    public static class Configuration implements Processor.Configuration {
        private String field;
        private String targetField;
        private List<String> includeKeys;
        private List<String> excludeKeys;
        private String fieldSplit = " ";
        private String valueSplit = "=";
        private boolean allowDuplicateValues = true;
        private boolean includeBrackets = true;
        private String prefix = "";
        private boolean recursive = false;
        private String trim;
        private String trimKey;

        public Configuration() { }

        public String getField() {
            return field;
        }

        public String getTargetField() {
            return targetField;
        }

        public List<String> getIncludeKeys() {
            return includeKeys;
        }

        public List<String> getExcludeKeys() {
            return excludeKeys;
        }

        public String getFieldSplit() {
            return fieldSplit;
        }

        public String getValueSplit() {
            return valueSplit;
        }

        public boolean isAllowDuplicateValues() {
            return allowDuplicateValues;
        }

        public boolean isIncludeBrackets() {
            return includeBrackets;
        }

        public String getPrefix() {
            return prefix;
        }

        public boolean isRecursive() {
            return recursive;
        }

        public String getTrim() {
            return trim;
        }

        public String getTrimKey() {
            return trimKey;
        }
    }
}
