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
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.logz.sawmill.FieldType.STRING;
import static org.apache.commons.collections4.CollectionUtils.isEmpty;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;

public final class Grok {

    public static final String PATTERN_GROUP = "pattern";
    public static final String SUBNAME_GROUP = "subname";
    public static final String DEFINITION_GROUP = "definition";
    private static final Regex GROK_PATTERN_REGEX = new Regex("%\\{" +
            "(?<" + PATTERN_GROUP + ">[A-z0-9_-]+)" +
            "(?::(?<" + SUBNAME_GROUP + ">[A-z0-9_:.-]+))?" +
            "(?:=(?<" + DEFINITION_GROUP + ">(?:(?:[^{}]+|\\.+)+)+))?" +
            "\\}");
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


    private String getNamedGroupMatch(String name, Region region, String pattern) {
        try {
            int number = GROK_PATTERN_REGEX.nameToBackrefNumber(name.getBytes(StandardCharsets.UTF_8), 0,
                    name.getBytes(StandardCharsets.UTF_8).length, region);
            NamedGroupMatch namedGroupMatch = getNamedGroupMatch(name, region, pattern.getBytes(), number);
            return (String) namedGroupMatch.getValue();
        } catch (ValueException e) {
            return null;
        }
    }

    private NamedGroupMatch getNamedGroupMatch(String name, Region region, byte[] textAsBytes, int... matches) {
        List<String> matchValue = new ArrayList<>();
        for (int number : matches) {
            if (region.beg[number] >= 0) {
                matchValue.add(extractString(textAsBytes, region.beg[number], region.end[number]));
            }
        }

        return new NamedGroupMatch(name, matchValue);
    }

    public String parsePattern(String grokPattern) {
        byte[] grokPatternBytes = grokPattern.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = GROK_PATTERN_REGEX.matcher(grokPatternBytes);

        int result = matcher.search(0, grokPatternBytes.length, Option.NONE);
        if (result == -1) {
            return grokPattern;
        }

        Region region = matcher.getEagerRegion();
        String patternName = getNamedGroupMatch(PATTERN_GROUP, region, grokPattern);
        String subName = getNamedGroupMatch(SUBNAME_GROUP, region, grokPattern);
        String definition = getNamedGroupMatch(DEFINITION_GROUP, region, grokPattern);

        if (isNotEmpty(definition)) {
            addPattern(patternName, definition);
        }

        String pattern = patternBank.get(patternName);

        if (pattern == null) {
            throw new RuntimeException(String.format("failed to create grok, unknown " + Grok.PATTERN_GROUP + " [%s]", grokPattern));
        }

        String grokPart;
        if (namedOnly && isNotEmpty(subName)) {
            grokPart = String.format("(?<%s>%s)", subName, pattern);
        } else if (!namedOnly) {
            grokPart = String.format("(?<%s>%s)", patternName + String.valueOf(result), pattern);
        } else {
            grokPart = String.format("(?:%s)", pattern);
        }

        String start = extractString(grokPatternBytes, 0, result);
        String rest = extractString(grokPatternBytes, region.end[0], grokPatternBytes.length);
        return start + parsePattern(grokPart + rest);
    }

    private void addPattern(String patternName, String definition) {
        patternBank.put(patternName, definition);
    }

    public Map<String, Object> captures(String text) {
        Map<String, Object> fields = new HashMap<>();
        byte[] textAsBytes = text.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = compiledExpression.matcher(textAsBytes);
        int result = matcher.search(0, textAsBytes.length, Option.DEFAULT);
        if (result == -1) {
            return null;
        }
        if (compiledExpression.numberOfNames() == 0) {
            return fields;
        }

        Region region = matcher.getEagerRegion();
        for (Iterator<NameEntry> iterator = compiledExpression.namedBackrefIterator(); iterator.hasNext();) {
            NameEntry entry = iterator.next();
            String groupName = extractString(entry.name, entry.nameP, entry.nameEnd);
            NamedGroupMatch namedGroupMatch = getNamedGroupMatch(groupName, region, textAsBytes, entry.getBackRefs());
            fields.put(namedGroupMatch.getName(), namedGroupMatch.getValue());
        }

        return fields;
    }

    private String extractString(byte[] original, int start, int end) {
        try {
            return new String(original, start, end - start, StandardCharsets.UTF_8);
        } catch (StringIndexOutOfBoundsException e) {
            return null;
        }
    }

    final class NamedGroupMatch {
        private final String fieldName;
        private final FieldType type;
        private final List<String> groupValue;

        public NamedGroupMatch(String groupName, List<String> groupValue) {
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
            if (isEmpty(groupValue)) { return null; }

            if (groupValue.size() == 1) {
                return convertValue(groupValue.get(0));
            } else {
                return groupValue.stream().map(this::convertValue).collect(Collectors.toList());
            }
        }

        private Object convertValue(String value) {
            Object valueAfterConvert = type.convertFrom(value, 0l);
            if (valueAfterConvert == null) {
                return groupValue;
            }

            return valueAfterConvert;
        }
    }
}

