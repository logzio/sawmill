package io.logz.sawmill.processors;

import com.samskivert.mustache.Template;
import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@ProcessorProvider(type = "kv", factory = KeyValueProcessor.Factory.class)
public class KeyValueProcessor implements Processor {

    private final Template field;
    private final Template targetField;
    private final Pattern pattern;
    private final List<String> includeKeys;
    private final List<String> excludeKeys;
    private final boolean allowDuplicateValues;
    private final String prefix;
    private final boolean recursive;
    private final String trim;
    private final String trimKey;

    public KeyValueProcessor(Template field,
                             Template targetField,
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
        this.pattern = buildPattern(fieldSplit, valueSplit, includeBrackets);
        this.includeKeys = includeKeys;
        this.excludeKeys = excludeKeys;
        this.allowDuplicateValues = allowDuplicateValues;
        this.prefix = prefix;
        this.recursive = recursive;
        this.trim = trim;
        this.trimKey = trimKey;
    }

    /***
     *  Build KeyValue Pattern
     *  Group 1: Key
     *  Group 2: Double quotes
     *  Group 3: Single quotes
     *  Group 4: Round brackets
     *  Group 5: Brackets
     *  Group 6: Angle beackets
     *  Group 7: Normal
     * @param includeBrackets
     * @param fieldSplit
     * @param valueSplit
     * @return KV Pattern
     */
    private Pattern buildPattern(String fieldSplit, String valueSplit, boolean includeBrackets) {
        String valueRxString = "(?:\"([^\"]+)\"|'([^']+)'";
        if (includeBrackets) {
            valueRxString += "|\\(([^\\)]+)\\)|\\[([^\\]]+)\\]|<([^>]+)>";
        }
        valueRxString += "|((?:\\\\ |[^" + fieldSplit + "])+))";

        return Pattern.compile("((?:\\\\ |[^" + fieldSplit + valueSplit + "])+)\\s*[" + valueSplit + "]\\s*" + valueRxString);
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
            kvMap.forEach(doc::addField);
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
                        ((List) oldVal).add(value);
                        return oldVal;
                    }
                    return new ArrayList<>(Arrays.asList(oldVal, value));
                });
            } else {
                kvMap.putIfAbsent(key, value);
            }
        }

        return kvMap;
    }

    private String getMatchedValue(Matcher matcher) {
        // Running all over the value options and take the one that captured
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

            return new KeyValueProcessor(TemplateService.compileTemplate(keyValueConfig.getField()),
                    TemplateService.compileTemplate(keyValueConfig.getTargetField()),
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
