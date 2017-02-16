package io.logz.sawmill.utilities;

import io.logz.sawmill.FieldType;
import org.joni.Matcher;
import org.joni.NameEntry;
import org.joni.Option;
import org.joni.Regex;
import org.joni.Region;
import org.joni.exception.ValueException;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.FieldType.STRING;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public final class Grok {

    private static final String PATTERN_GROUP = "pattern";
    private static final String SUBNAME_GROUP = "subname";
    private static final String DEFINITION_GROUP = "definition";
    private static final Regex GROK_PATTERN_REGEX = new Regex("%\\{" +
            "(?<pattern>[A-z0-9]+)" +
            "(?::(?<subname>[A-z0-9_:.-]+))?" +
            "(?:=(?<definition>" +
            "(?:" +
            "(?:[^{}]+|\\.+)+" +
            ")+" +
            ")" +
            ")?" + "\\}");
    private final Map<String, String> patternBank;
    private final boolean namedOnly;
    private final Regex compiledExpression;

    public Grok(Map<String, String> patternBank, String grokPattern) {
        this(patternBank, grokPattern, true);
    }

    public Grok(Map<String, String> patternBank, String grokPattern, boolean namedOnly) {
        this.patternBank = patternBank;
        this.namedOnly = namedOnly;
        this.compiledExpression = compilePattern(grokPattern);
    }

    private Regex compilePattern(String grokPattern) {
        return new Regex(parsePattern(grokPattern));
    }


    public String getNamedGroupMatch(String name, Region region, String pattern) {
        try {
            int number = GROK_PATTERN_REGEX.nameToBackrefNumber(name.getBytes(StandardCharsets.UTF_8), 0,
                    name.getBytes(StandardCharsets.UTF_8).length, region);
            int begin = region.beg[number];
            int end = region.end[number];
            return new String(pattern.getBytes(StandardCharsets.UTF_8), begin, end - begin, StandardCharsets.UTF_8);
        } catch (StringIndexOutOfBoundsException e) {
            return null;
        } catch (ValueException e) {
            return null;
        }
    }

    public String parsePattern(String grokPattern) {
        byte[] grokPatternBytes = grokPattern.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = GROK_PATTERN_REGEX.matcher(grokPatternBytes);

        int result = matcher.search(0, grokPatternBytes.length, Option.NONE);
        if (result != -1) {
            Region region = matcher.getEagerRegion();
            String subName = getNamedGroupMatch(SUBNAME_GROUP, region, grokPattern);
            String definition = getNamedGroupMatch(DEFINITION_GROUP, region, grokPattern);
            String patternName = getNamedGroupMatch(PATTERN_GROUP, region, grokPattern);

            if (isNotEmpty(definition)) {
                addPattern(patternName, definition);
            }

            String pattern = patternBank.get(patternName);

            if (pattern == null) {
                throw new RuntimeException(String.format("failed to create grok, unknown pattern [%s]", grokPattern));
            }

            String grokPart;
            if (namedOnly && isNotEmpty(subName)) {
                grokPart = String.format("(?<%s>%s)", subName, pattern);
            } else if (!namedOnly) {
                grokPart = String.format("(?<%s>%s)", patternName + String.valueOf(result), pattern);
            } else {
                grokPart = String.format("(?:%s)", pattern);
            }

            String start = new String(grokPatternBytes, 0, result, StandardCharsets.UTF_8);
            String rest = new String(grokPatternBytes, region.end[0], grokPatternBytes.length - region.end[0], StandardCharsets.UTF_8);
            return start + parsePattern(grokPart + rest);
        }

        return grokPattern;
    }

    public void addPattern(String patternName, String definition) {
        patternBank.put(patternName, definition);
    }

    public Map<String, Object> captures(String text) {
        byte[] textAsBytes = text.getBytes(StandardCharsets.UTF_8);
        Map<String, Object> fields = new HashMap<>();
        Matcher matcher = compiledExpression.matcher(textAsBytes);
        int result = matcher.search(0, textAsBytes.length, Option.DEFAULT);
        if (result != -1 && compiledExpression.numberOfNames() > 0) {
            Region region = matcher.getEagerRegion();
            for (Iterator<NameEntry> entry = compiledExpression.namedBackrefIterator(); entry.hasNext();) {
                NameEntry e = entry.next();
                String groupName = new String(e.name, e.nameP, e.nameEnd - e.nameP, StandardCharsets.UTF_8);
                for (int number : e.getBackRefs()) {
                    if (region.beg[number] >= 0) {
                        String matchValue = new String(textAsBytes, region.beg[number], region.end[number] - region.beg[number],
                            StandardCharsets.UTF_8);
                        NamedGroupMatch namedGroupMatch = new NamedGroupMatch(groupName, matchValue);
                        Object value = namedGroupMatch.getValue();
                        fields.compute(namedGroupMatch.getName(), (k, oldVal) -> {
                            if (oldVal == null) return value;
                            if (oldVal instanceof List) {
                                ((List) oldVal).add(value);
                                return oldVal;
                            }
                            return new ArrayList<>(Arrays.asList(oldVal, value));
                        });
                    }
                }

            }
            return fields;
        } else if (result != -1) {
            return fields;
        }
        return null;
    }

    final class NamedGroupMatch {
        private final String fieldName;
        private final FieldType type;
        private final String groupValue;

        public NamedGroupMatch(String groupName, String groupValue) {
            String[] parts = groupName.split(":");
            fieldName = parts[0];

            if (parts.length == 2) {
                type = FieldType.tryParseOrDefault(parts[1]);
            } else {
                type = STRING;
            }

            this.groupValue = groupValue;
        }

        public String getName() {
            return fieldName;
        }

        public Object getValue() {
            if (groupValue == null) { return null; }

            Object valueAfterConvert = type.convertFrom(groupValue, 0l);
            if (valueAfterConvert == null) { return groupValue; }

            return valueAfterConvert;
        }
    }
}

