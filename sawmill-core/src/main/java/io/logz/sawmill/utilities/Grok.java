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
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import static io.logz.sawmill.FieldType.STRING;
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

    private String parsePattern(String grokPattern) {
        byte[] grokPatternBytes = grokPattern.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = GROK_PATTERN_REGEX.matcher(grokPatternBytes);

        int result = matcher.search(0, grokPatternBytes.length, Option.NONE);
        boolean matchNotFound = result == -1;
        if (matchNotFound) {
            return grokPattern;
        }

        Region region = matcher.getEagerRegion();
        String patternName = matchPatternValue(PATTERN_GROUP, region, grokPattern);
        String subName = matchPatternValue(SUBNAME_GROUP, region, grokPattern);
        String definition = matchPatternValue(DEFINITION_GROUP, region, grokPattern);

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

    public List<Match> matches(String text) {
        List<Match> matches = new ArrayList<>();
        byte[] textAsBytes = text.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = compiledExpression.matcher(textAsBytes);
        int result = matcher.search(0, textAsBytes.length, Option.DEFAULT);
        boolean matchNotFound = result == -1;
        if (matchNotFound) {
            return null;
        }
        if (compiledExpression.numberOfNames() == 0) {
            return matches;
        }

        Region region = matcher.getEagerRegion();
        for (Iterator<NameEntry> iterator = compiledExpression.namedBackrefIterator(); iterator.hasNext();) {
            NameEntry entry = iterator.next();
            String groupName = extractString(entry.name, entry.nameP, entry.nameEnd);
            int[] matchNumbers = entry.getBackRefs();

            matches.add(match(groupName, region, textAsBytes, matchNumbers));
        }

        return matches;
    }

    private String matchPatternValue(String groupName, Region region, String pattern) {
        try {
            int matchNumber = GROK_PATTERN_REGEX.nameToBackrefNumber(groupName.getBytes(StandardCharsets.UTF_8), 0,
                    groupName.getBytes(StandardCharsets.UTF_8).length, region);
            Match match = match(groupName, region, pattern.getBytes(), matchNumber);
            return (String) match.getValue();
        } catch (ValueException e) {
            return null;
        }
    }

    private Match match(String groupName, Region region, byte[] textAsBytes, int... matchNumbers) {
        String[] parts = groupName.split(":");
        String fieldName = parts[0];
        FieldType type = parts.length == 2 ? FieldType.tryParseOrDefault(parts[1]) : STRING;
        Object value = getMatchValue(region, textAsBytes, matchNumbers, type);

        int start = region.beg[matchNumbers[0]];
        int end = region.end[matchNumbers[matchNumbers.length - 1]];

        return new Match(fieldName, value, start, end);
    }

    private Object getMatchValue(Region region, byte[] textAsBytes, int[] matchNumbers, FieldType type) {
        if (matchNumbers.length == 1) {
            String value = extractString(textAsBytes, region.beg[matchNumbers[0]], region.end[matchNumbers[0]]);
            if (value == null) return null;
            return convertValue(value, type);
        }

        List<String> listValue = new ArrayList<>();
        for (int number : matchNumbers) {
            if (region.beg[number] >= 0) {
                listValue.add(extractString(textAsBytes, region.beg[number], region.end[number]));
            }
        }
        if (listValue.isEmpty()) return null;
        return listValue.stream().map(v -> convertValue(v, type)).collect(Collectors.toList());
    }

    private Object convertValue(String value, FieldType type) {
        Object valueAfterConvert = type.convertFrom(value, 0l);
        if (valueAfterConvert == null) {
            return value;
        }

        return valueAfterConvert;
    }

    private String extractString(byte[] original, int start, int end) {
        try {
            return new String(original, start, end - start, StandardCharsets.UTF_8);
        } catch (StringIndexOutOfBoundsException e) {
            return null;
        }
    }

    public final class Match {
        private final String name;
        private final Object value;
        private final int start;
        private final int end;

        public Match(String name, Object value, int start, int end) {
            this.name = name;
            this.value = value;
            this.start = start;
            this.end = end;
        }

        public String getName() {
            return name;
        }

        public Object getValue() {
            return value;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }
    }
}

