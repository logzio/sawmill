package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ProcessorProvider(type = "kv", factory = KeyValueProcessor.Factory.class)
public class KeyValueProcessor implements Processor {

    private final String field;
    private final String targetField;
    private final Pattern pattern;
    private final List<String> includeKeys;
    private final List<String> excludeKeys;
    private final boolean allowDuplicateValues;
    private final String prefix;
    private final boolean recursive;
    private final String trim;
    private final String trimKey;

    public KeyValueProcessor(String field,
                             String targetField,
                             Pattern pattern,
                             List<String> includeKeys,
                             List<String> excludeKeys,
                             boolean allowDuplicateValues,
                             String prefix,
                             boolean recursive,
                             String trim,
                             String trimKey) {
        this.field = field;
        this.targetField = targetField;
        this.pattern = pattern;
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
        this.allowDuplicateValues = allowDuplicateValues;
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

        Map<String, Object> kvMap = new HashMap<>();

        Object kvField = doc.getField(this.field);

        if (kvField instanceof List) {
            for (Object subField : (List) kvField) {
                if (subField instanceof String) {
                    kvMap.putAll(parse((String) subField));
                }
            }

        } else if (kvField instanceof String) {
            kvMap = parse((String) kvField);
        } else {
            return ProcessResult.failure(String.format("failed to process kv, cannot parse type [%s] of field [%s]", kvField.getClass(), field));
        }

        if (includeKeys != null) {
            kvMap.keySet().retainAll(includeKeys);
        }
        if (excludeKeys != null) {
            kvMap.keySet().removeAll(excludeKeys);
        }

        if (targetField != null) {
            doc.addField(targetField, kvMap);
        } else {
            kvMap.forEach((key,value) -> {
                doc.addField(key, value);
            });
        }

        return ProcessResult.success();
    }

    private String getKey(Matcher matcher) {
        return prefix + trim(matcher.group(1), trimKey);
    }

    private Object getValue(Matcher matcher) {
        Object value = getMatchedValue(matcher);

        if (recursive) {
            Map<String,Object> innerKv = parse((String) value);
            if (MapUtils.isNotEmpty(innerKv)) {
                return innerKv;
            }
        }

        return trim((String) value, trim);
    }

    private Map<String,Object> parse(String message) {
        Map<String,Object> kvMap = new HashMap<>();

        Matcher matcher = pattern.matcher(message);

        while (matcher.find()) {
            String key = getKey(matcher);
            Object value = getValue(matcher);

            if (allowDuplicateValues) {
                kvMap.compute(key, (k, oldVal) -> {
                    if (oldVal == null) return value;
                    if (oldVal instanceof List) {
                        return ((List) oldVal).add(value);
                    }
                    return Arrays.asList(oldVal, value);
                });
            } else {
                kvMap.putIfAbsent(key, value);
            }
        }

        return kvMap;
    }

    private String getMatchedValue(Matcher matcher) {
        for (int i=2; i <= 7; i++ ) {
            String value = matcher.group(i);
            if (value != null) {
                return value;
            }
        }
        return null;
    }

    private String trim(String key, String trimChars) {
        if (trimChars != null) {
            key = StringUtils.strip(key, trimChars);
        }
        return key.trim();
    }

    public static class Factory implements Processor.Factory {
        public Factory() {
        }

        @Override
        public KeyValueProcessor create(Map<String,Object> config) {
            KeyValueProcessor.Configuration keyValueConfig = JsonUtils.fromJsonMap(KeyValueProcessor.Configuration.class, config);

            String valueRxString = "(?:\"([^\"]+)\"|'([^']+)'";
            if (keyValueConfig.isIncludeBrackets()) {
                valueRxString += "|\\(([^\\)]+)\\)|\\[([^\\]]+)\\]|<([^>]+)>";
            }
            valueRxString += "|((?:\\\\ |[^" + keyValueConfig.getFieldSplit() + "])+))";
            Pattern pattern = Pattern.compile("((?:\\\\ |[^" + keyValueConfig.getFieldSplit() + keyValueConfig.getValueSplit() + "])+)\\s*[" + keyValueConfig.getValueSplit() + "]\\s*" + valueRxString);

            return new KeyValueProcessor(keyValueConfig.getField(),
                    keyValueConfig.getTargetField(),
                    pattern,
                    keyValueConfig.getIncludeKeys(),
                    keyValueConfig.getExcludeKeys(),
                    keyValueConfig.isAllowDuplicateValues(),
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
