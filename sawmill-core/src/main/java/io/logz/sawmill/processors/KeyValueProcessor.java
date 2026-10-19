package io.logz.sawmill.processors;

import io.logz.sawmill.Doc;
import io.logz.sawmill.ProcessResult;
import io.logz.sawmill.Processor;
import io.logz.sawmill.Template;
import io.logz.sawmill.TemplateService;
import io.logz.sawmill.annotations.ProcessorProvider;
import io.logz.sawmill.utilities.JsonUtils;
import org.apache.commons.collections4.MapUtils;
import org.apache.commons.lang3.StringUtils;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;

import javax.inject.Inject;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

import static org.joni.Matcher.INTERRUPTED;

import static java.util.Objects.requireNonNull;

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
    private final Template targetField;
    private final Regex pattern;
    private final int keyGroupNumber;
    private final int[] valueGroupNumbers;
    private final Set<String> includeKeys;
    private final Set<String> excludeKeys;
    private final boolean allowDuplicateValues;
    private final String prefix;
    private final boolean recursive;
    private final String trim;
    private final String trimKey;
    private final int maxKeyLength;

    public KeyValueProcessor(String field,
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
                             String trimKey,
                             int maxKeyLength) {
        this.field = requireNonNull(field, "field cannot be null");
        this.targetField = targetField;
        this.pattern = buildPattern(fieldSplit, valueSplit, includeBrackets);
        this.keyGroupNumber = pattern.nameToBackrefNumber(KEY.getBytes(), 0, KEY.getBytes().length, null);
        this.valueGroupNumbers = valueGroupNumbers(pattern);
        // sets: retainAll/removeAll call contains() once per parsed key
        this.includeKeys = includeKeys == null ? null : new HashSet<>(includeKeys);
        this.excludeKeys = excludeKeys == null ? null : new HashSet<>(excludeKeys);
        this.allowDuplicateValues = allowDuplicateValues;
        this.prefix = prefix;
        this.recursive = recursive;
        this.trim = trim;
        this.trimKey = trimKey;
        this.maxKeyLength = maxKeyLength;
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
    public ProcessResult process(Doc doc) throws InterruptedException {
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
            doc.addField(targetField.render(doc), kvMap);
        } else {
            kvMap.forEach(doc::addField);
        }

        return ProcessResult.success();
    }

    private String getKey(byte[] message, Region region) {
        String trimmed = trim(extractString(message, region.beg[keyGroupNumber], region.end[keyGroupNumber]), trimKey);
        // "" + key would copy the key; a null prefix still concatenates to "null...", as before
        String key = "".equals(prefix) ? trimmed : prefix + trimmed;
        return key.length() <= maxKeyLength ? key : null;
    }

    private Object getValue(byte[] message, Region region) throws InterruptedException {
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

    private Map<String,Object> parse(String message) throws InterruptedException {
        Map<String,Object> kvMap = new HashMap<>();
        int matchesCounter = 0;

        byte[] messageAsBytes = message.getBytes();
        Matcher matcher = pattern.matcher(messageAsBytes);

        int result = matcher.searchInterruptible(0, messageAsBytes.length, Option.MULTILINE);

        while (result != -1 && matchesCounter < MAX_MATCHES) {
            Region region = matcher.getEagerRegion();
            String key = getKey(messageAsBytes, region);

            if (key != null) {

                Object value = getValue(messageAsBytes, region);

                if (value != null) {

                    if (allowDuplicateValues) {
                        // merge, not putIfAbsent: HashMap resizes before inserting in merge and compute but after
                        // in put, so keeping compute's timing keeps the map's iteration order, and with it the order
                        // fields reach the doc
                        kvMap.merge(key, value, APPEND_VALUE);
                    } else {
                        kvMap.putIfAbsent(key, value);
                    }
                }
            }
            int endOfFullMatch = region.end[0];
            result = matcher.searchInterruptible(endOfFullMatch, messageAsBytes.length, Option.MULTILINE);

            matchesCounter++;

            if (result == INTERRUPTED) {
                throw new InterruptedException();
            }
        }

        return kvMap;
    }

    private static final BiFunction<Object, Object, Object> APPEND_VALUE = (oldVal, value) -> {
        if (oldVal instanceof List) {
            ((List) oldVal).add(value);
            return oldVal;
        }
        return new ArrayList<>(Arrays.asList(oldVal, value));
    };

    private String getMatchedValue(byte[] message, Region region) {
        // the value alternative that captured
        for (int number : valueGroupNumbers) {
            if (region.beg[number] >= 0) {
                return extractString(message, region.beg[number], region.end[number]);
            }
        }
        return null;
    }

    /** Value group numbers in the order of the named groups after the key, resolved once per pattern. */
    private static int[] valueGroupNumbers(Regex pattern) {
        List<Integer> numbers = new ArrayList<>();
        Iterator<NameEntry> iterator = pattern.namedBackrefIterator();
        iterator.next(); // the key
        while (iterator.hasNext()) {
            for (int number : iterator.next().getBackRefs()) {
                numbers.add(number);
            }
        }
        return numbers.stream().mapToInt(Integer::intValue).toArray();
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
        private final TemplateService templateService;

        @Inject
        public Factory(TemplateService templateService) {
            this.templateService = templateService;
        }

        @Override
        public KeyValueProcessor create(Map<String,Object> config) {
            KeyValueProcessor.Configuration keyValueConfig = JsonUtils.fromJsonMap(KeyValueProcessor.Configuration.class, config);

            Template targetField = StringUtils.isEmpty(keyValueConfig.getTargetField()) ? null : templateService.createTemplate(keyValueConfig.getTargetField());
            return new KeyValueProcessor(keyValueConfig.getField(),
                    targetField,
                    keyValueConfig.getIncludeKeys(),
                    keyValueConfig.getExcludeKeys(),
                    keyValueConfig.getFieldSplit(),
                    keyValueConfig.getValueSplit(),
                    keyValueConfig.isAllowDuplicateValues(),
                    keyValueConfig.isIncludeBrackets(),
                    keyValueConfig.getPrefix(),
                    keyValueConfig.isRecursive(),
                    keyValueConfig.getTrim(),
                    keyValueConfig.getTrimKey(),
                    keyValueConfig.getMaxKeyLength());
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
        private int maxKeyLength = 40;

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

        public int getMaxKeyLength() {
            return maxKeyLength;
        }
    }
}
