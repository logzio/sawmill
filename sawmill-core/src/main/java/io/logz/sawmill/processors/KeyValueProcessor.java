package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@ProcessorProvider(type = "kv", factory = KeyValueProcessor.Factory.class)
public class KeyValueProcessor implements Processor {

    public static final String KEY = "key";
    public static final String DOUBLE_QUOTE = "double-quote";
    public static final String SINGLE_QUOTE = "single-quote";
    public static final String ROUND_BRACKETS = "round-brackets";
    public static final String BRACKETS = "brackets";
    public static final String ANGLE_BRACKETS = "angle-brackets";
    public static final String NORMAL = "normal";
    public static final int MAX_MATCHES = 1000;
    private final String field;
    private final String targetField;
    private final Regex pattern;
    private final List<String> includeKeys;
    private final List<String> excludeKeys;
    private final boolean allowDuplicateValues;
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
    private Regex buildPattern(String fieldSplit, String valueSplit, boolean includeBrackets) {
        String valueRegexString = "(?:\"(?<" + DOUBLE_QUOTE + ">[^\"]+)\"|'(?<" + SINGLE_QUOTE + ">[^']+)'";
        if (includeBrackets) {
            valueRegexString += "|\\((?<" + ROUND_BRACKETS + ">[^\\)]+)\\)|\\[(?<" + BRACKETS + ">[^\\]]+)\\]|<(?<" + ANGLE_BRACKETS + ">[^>]+)>";
        }
        valueRegexString += "|(?<" + NORMAL + ">(?:\\\\ |[^" + fieldSplit + "])+))";

        String patternString = "(?<" + KEY + ">(?:\\\\ |[^" + fieldSplit + valueSplit + "])+)\\s*[" + valueSplit + "]\\s*" + valueRegexString;
        byte[] bytes = patternString.getBytes();
        return new Regex(bytes, 0, bytes.length, Option.MULTILINE);
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

    private String getKey(byte[] message, Region region) {
        int matchNumber = pattern.nameToBackrefNumber(KEY.getBytes(), 0, KEY.getBytes().length, region);
        return prefix + trim(extractString(message, region.beg[matchNumber], region.end[matchNumber]), trimKey);
    }

    private Object getValue(byte[] message, Region region) {
        String value = getMatchedValue(message, region);

        if (value == null) {
            return null;
        }

        if (recursive) {
            Map<String,Object> innerKv = parse(value);
            if (MapUtils.isNotEmpty(innerKv)) {
                return innerKv;
            }
        }

        return trim(value, trim);
    }

    private Map<String,Object> parse(String message) {
        Map<String,Object> kvMap = new HashMap<>();
        int matchesCounter = 0;

        byte[] messageAsBytes = message.getBytes();
        Matcher matcher = pattern.matcher(messageAsBytes);

        int result = matcher.search(0, messageAsBytes.length, Option.MULTILINE);

        while (result != -1 && matchesCounter < MAX_MATCHES) {
            Region region = matcher.getEagerRegion();
            String key = getKey(messageAsBytes, region);
            Object value = getValue(messageAsBytes, region);

            if (value == null) {
                continue;
            }

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

            int endOfFullMatch = region.end[0];
            result = matcher.search(endOfFullMatch, messageAsBytes.length, Option.MULTILINE);

            matchesCounter++;
        }

        return kvMap;
    }

    private String getMatchedValue(byte[] message, Region region) {
        Iterator<NameEntry> iterator = pattern.namedBackrefIterator();

        // Skip the key
        iterator.next();

        // Running all over the value options and take the one that captured
        while (iterator.hasNext()) {
            NameEntry entry = iterator.next();
            for (int number : entry.getBackRefs()) {
                if (region.beg[number] >= 0) {
                    return extractString(message, region.beg[number], region.end[number]);
                }
            }
        }
        return null;
    }

    private String extractString(byte[] original, int start, int end) {
        try {
            return new String(original, start, end - start, StandardCharsets.UTF_8);
        } catch (StringIndexOutOfBoundsException e) {
            return null;
        }
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
