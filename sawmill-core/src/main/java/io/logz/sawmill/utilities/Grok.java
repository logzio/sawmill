package io.logz.sawmill.utilities;

import io.logz.sawmill.FieldType;
import org.apache.commons.lang3.StringUtils;
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

import static io.logz.sawmill.FieldType.STRING;
import static org.apache.commons.lang3.StringUtils.isNotEmpty;
import static org.joni.Matcher.INTERRUPTED;

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
    private final List<CaptureGroup> captureGroups;

    public Grok(Map<String, String> patternBank, String grokPattern) {
        this(patternBank, grokPattern, true);
    }

    public Grok(Map<String, String> patternBank, String grokPattern, boolean namedOnly) {
        this.patternBank = patternBank;
        this.namedOnly = namedOnly;
        this.compiledExpression = compilePattern(grokPattern);
        this.captureGroups = captureGroups(compiledExpression);
    }

    /** Field name, type and group numbers of each named capture, resolved once instead of on every match. */
    private List<CaptureGroup> captureGroups(Regex regex) {
        List<CaptureGroup> groups = new ArrayList<>();
        if (regex.numberOfNames() == 0) {
            return groups;
        }
        for (Iterator<NameEntry> iterator = regex.namedBackrefIterator(); iterator.hasNext();) {
            NameEntry entry = iterator.next();
            groups.add(CaptureGroup.of(extractString(entry.name, entry.nameP, entry.nameEnd), entry.getBackRefs()));
        }
        return groups;
    }

    private Regex compilePattern(String grokPattern) {
        byte[] bytes = parsePattern(grokPattern).getBytes();
        return new Regex(bytes, 0, bytes.length, Option.MULTILINE);
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

    public List<Match> matches(String text) throws InterruptedException {
        if (Thread.interrupted()) {
            throw new InterruptedException();
        }

        List<Match> matches = new ArrayList<>();
        byte[] textAsBytes = text.getBytes(StandardCharsets.UTF_8);
        Matcher matcher = compiledExpression.matcher(textAsBytes);
        int result = matcher.searchInterruptible(0, textAsBytes.length, Option.MULTILINE);
        boolean matchNotFound = result == -1;
        if (matchNotFound) {
            return null;
        }
        if (result == INTERRUPTED) {
            throw new InterruptedException();
        }

        Region region = matcher.getEagerRegion();
        for (CaptureGroup group : captureGroups) {
            matches.add(match(group, region, textAsBytes));
        }

        return matches;
    }

    private String matchPatternValue(String groupName, Region region, String pattern) {
        try {
            int matchNumber = GROK_PATTERN_REGEX.nameToBackrefNumber(groupName.getBytes(StandardCharsets.UTF_8), 0,
                    groupName.getBytes(StandardCharsets.UTF_8).length, region);
            Match match = match(CaptureGroup.of(groupName, matchNumber), region, pattern.getBytes());
            List<Object> values = match.getValues();
            return values.size() == 0 ? null : (String) values.get(0);
        } catch (ValueException e) {
            return null;
        }
    }

    private Match match(CaptureGroup group, Region region, byte[] textAsBytes) {
        List<Offset> offsets = getOffsets(region.beg, region.end, group.backRefs);
        List<MatchValue> values = getMatchValue(textAsBytes, offsets, group.type);

        return new Match(group.fieldName, values);
    }

    private List<Offset> getOffsets(int[] beg, int[] end, int[] matchNumbers) {
        List<Offset> offsets = new ArrayList<>();
        for (int matchNumber : matchNumbers) {
            offsets.add(new Offset(beg[matchNumber], end[matchNumber]));
        }
        return offsets;
    }

    private List<MatchValue> getMatchValue(byte[] textAsBytes, List<Offset> offsets, FieldType type) {
        List<MatchValue> matchValues = new ArrayList<>();
        for (Offset offset : offsets) {
            if (offset.getStart() >= 0) {
                String rawValue = extractString(textAsBytes, offset.getStart(), offset.getEnd());
                if (!StringUtils.isEmpty(rawValue)) {
                    Object convertedValue = convertValue(rawValue, type);
                    matchValues.add(new MatchValue(convertedValue, offset));
                }
            }
        }
        return matchValues;
    }

    private Object convertValue(String value, FieldType type) {
        Object valueAfterConvert = type.convertFrom(value, 0L);
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

    private static final class CaptureGroup {
        private final String fieldName;
        private final FieldType type;
        private final int[] backRefs;

        private CaptureGroup(String fieldName, FieldType type, int[] backRefs) {
            this.fieldName = fieldName;
            this.type = type;
            this.backRefs = backRefs;
        }

        /** {@code groupName} is the field name, optionally followed by {@code :type}. */
        private static CaptureGroup of(String groupName, int... backRefs) {
            String[] parts = groupName.split(":");
            FieldType type = parts.length == 2 ? FieldType.tryParseOrDefault(parts[1]) : STRING;
            return new CaptureGroup(parts[0], type, backRefs);
        }
    }

    public final class Match {
        private final String name;
        private final List<MatchValue> values;

        public Match(String name, List<MatchValue> values) {
            this.name = name;
            this.values = values;
        }

        public String getName() {
            return name;
        }

        public List<MatchValue> getMatchValues() {
            return values;
        }

        public List<Object> getValues() {
            List<Object> result = new ArrayList<>(values.size());
            for (MatchValue value : values) {
                result.add(value.getValue());
            }
            return result;
        }
    }

    public final class MatchValue {
        private final Object value;
        private final Offset offset;

        public MatchValue(Object value, Offset offset) {
            this.value = value;
            this.offset = offset;
        }

        public Object getValue() {
            return value;
        }

        public int getStart() {
            return offset.getStart();
        }

        public int getEnd() {
            return offset.getEnd();
        }
    }

    private final class Offset {
        private final int start;
        private final int end;

        public Offset(int start, int end) {
            this.start = start;
            this.end = end;
        }

        public int getStart() {
            return start;
        }

        public int getEnd() {
            return end;
        }
    }
}

