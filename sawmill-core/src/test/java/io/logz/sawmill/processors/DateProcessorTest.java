package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import io.logz.sawmill.exceptions.ProcessorConfigurationException;
import io.logz.sawmill.utilities.JsonUtils;
import org.junit.Test;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeFormatterBuilder;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Random;

import static io.logz.sawmill.FieldType.DOUBLE;
import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

public class DateProcessorTest {

    @Test
    public void testUnixMsFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli());

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }
    @Test
    public void testUnixSFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        Double dateTimeAsEpocSecondsWithMili = 1557315444.223578;
        Doc doc = createDoc("datetime",dateTimeAsEpocSecondsWithMili);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);
        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();

        ZonedDateTime zonedDateTime =  ZonedDateTime.ofInstant(Instant.ofEpochMilli(((Double)(dateTimeAsEpocSecondsWithMili*1000)).longValue()),zoneId);
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixMsFormatString() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, String.valueOf(zonedDateTime.toInstant().toEpochMilli()));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatWithDoubleValue() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000d);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatString() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, String.valueOf(zonedDateTime.toInstant().toEpochMilli() / 1000));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testIso8601Format() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        String iso8601Format1 = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS"));
        String iso8601Format2 = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSxxx"));
        Doc doc = createDoc(field, iso8601Format1);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("ISO8601"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));

        doc = createDoc(field, iso8601Format2);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testSeveralISOPatterns() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        List<String> formats = Arrays.asList("dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd HH:mm:ss.SSSSS", "ddMMyyyy HHmmssSSS");

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", formats,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        formats.forEach(format -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            String dateString = zonedDateTime.format(formatter);
            Doc doc = createDoc(field, dateString);

            ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

            assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
            assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
        });
    }

    @Test
    public void testParseInvalidObjects() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        Doc docWithMap = createDoc(field, ImmutableMap.of("its", "a", "map", "should", "not", "work"));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(docWithMap).isSucceeded()).isFalse();

        Doc docWithList = createDoc(field, Arrays.asList("its", "a", "list", "should", "not", "work"));

        assertThat(dateProcessor.process(docWithList).isSucceeded()).isFalse();
    }

    @Test
    public void testCaseInsensitive() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern("dd/MMM/yyyy:HH:mm:ss Z").toFormatter();
        String dateString = zonedDateTime.format(formatter).toLowerCase();
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss Z"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testISOFormatWithoutTimezone() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
        String dateString = zonedDateTime.format(formatter);
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss Z"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testISOFormatWithTimezone() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("Asia/Tokyo");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
        String dateString = zonedDateTime.format(formatter);
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss"),
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testISOFormatWithNumberValueInField() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isFalse();
        assertThat(doc.hasField(targetField)).isFalse();
    }

    @Test
    public void testISOFormatWithoutTimezoneDefaultIsUTC() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss");
        String dateString = zonedDateTime.format(formatter);
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("dd/MMM/yyyy:HH:mm:ss"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);
        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatWithoutTimezoneDefaultIsUTC() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"));

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testUnixFormatStringWithoutTimezoneDefaultIsUTC() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, String.valueOf(zonedDateTime.toInstant().toEpochMilli() / 1000));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "timeZone", null);

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);
        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testBadConfigs() {
        assertThatThrownBy(() -> createProcessor(DateProcessor.class)).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "field", "aaaa")).isInstanceOf(ProcessorConfigurationException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "formats", Arrays.asList("aaaa"))).isInstanceOf(NullPointerException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "field", "bla", "formats", Arrays.asList("notValid"))).isInstanceOf(RuntimeException.class);
        assertThatThrownBy(() -> createProcessor(DateProcessor.class, "field", "bla", "formats", Arrays.asList("UNIX"), "outputFormat", "notValid")).isInstanceOf(RuntimeException.class);
    }
    @Test
    public void testOutputForamtUnix() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        String outputFormat = "UNIX";
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        String iso8601Format = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss,SSS"));
        Doc doc = createDoc(field, iso8601Format);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("ISO8601"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.UNIX));
    }

    @Test
    public void testOutputFormatElastic() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        String outputFormat = "ELASTIC";
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli());

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX_MS"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.ELASTIC));
    }

    @Test
    public void testOutputFormatCustom() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("Europe/Paris");
        String outputFormat = "yyyy-MM-ss HH:mm:ss Z";

        DateTimeFormatter customOutputFormatter = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(outputFormat).toFormatter();

        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("UNIX"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(customOutputFormatter));
    }

    @Test
    public void testCustomFormatToUnixMs() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        String format = zonedDateTime.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss,SSS"));
        String outputFormat = "UNIX_MS";
        Doc doc = createDoc(field, format);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", Arrays.asList("yyyy-MM-dd HH:mm:ss,SSS"),
                "outputFormat", outputFormat,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.UNIX_MS));
    }

    @Test
    public void testParsingMatchesParseBest() {
        List<List<String>> formatLists = Arrays.asList(
                Arrays.asList("ISO8601"),
                Arrays.asList("yyyy-MM-dd HH:mm:ss.SSS"),
                Arrays.asList("dd/MMM/yyyy:HH:mm:ss Z"),
                Arrays.asList("yyyy-MM-dd'T'HH:mm:ssXXX"),
                Arrays.asList("ELASTIC"),
                Arrays.asList("yyyy-MM-dd"),
                Arrays.asList("yyyy-MM-dd Z"),
                Arrays.asList("UNIX"),
                Arrays.asList("MMM dd HH:mm:ss"),
                Arrays.asList("yyyy-MM-dd HH:mm:ss.SSS", "ISO8601", "dd/MMM/yyyy:HH:mm:ss Z"));
        List<ZoneId> timeZones = Arrays.asList(null, ZoneId.of("Europe/Paris"));
        List<String> values = Arrays.asList(
                "2026-10-15T09:00:00.123Z", "2026-10-15T09:00:00", "2026-10-15T09:00:00+02:00", "2026-10-15t09:00:00z",
                "2026-10-15T09:00:00.123+05:30", "2026-10-15T09:00Z", "2026-10-15 09:00:00.123", "2026-02-30 09:00:00.000",
                "15/Oct/2026:09:00:00 +0200", "2026-10-15", "1792060391", "Oct 15 09:00:00", "2026-10-15T25:00:00Z",
                "2026-10-15T09:00:00+02:00[Europe/Paris]", "", "garbage",
                // offset out of range, with no date-time for the parser to reject it against
                "2026-10-15 +1900",
                // fast-path edges
                "2024-02-29 23:59:59.999", "2026-02-29 00:00:00.000", "2026-04-31 00:00:00.000", "2026-10-15 24:00:00.000",
                "2026-10-15 09:00:60.000", "0000-01-01 00:00:00.000", "2026-10-15 09:00:00.12", "2026-10-15 09:00:00.1234",
                "2026-10-15T09:00:00.123", "2026-03-29 02:30:00.000", "2026-10-25 02:30:00.000", " 2026-10-15 09:00:00.123",
                "2026-10-15T09:00:00.1", "2026-10-15T09:00:00.123456789Z", "2026-10-15T09:00:00.1234567891Z",
                "2026-10-15T09:00:00,5Z", "2026-10-15T09:00:00.Z", "2026-10-15T09:00:00123", "2026-10-15T09:00:00-00:00",
                "2026-10-15T09:00:00+18:00", "2026-10-15T09:00:00+18:30", "2026-10-15T09:00:00+0200", "2026-10-15T09:00:00+02",
                "2026-10-15T09:00:00+05:45", "2026-10-15T24:00:00Z", "2024-02-29T12:00:00Z", "2026-02-29T12:00:00Z",
                "2026-03-29T02:30:00", "\u0662\u0660\u0662\u0666-10-15 09:00:00.123");

        for (List<String> formats : formatLists) {
            for (ZoneId timeZone : timeZones) {
                DateProcessor processor = new DateProcessor("datetime", "@timestamp", formats, timeZone, "ELASTIC");
                for (String value : values) {
                    Doc doc = createDoc("datetime", value);
                    String expected = parseBestReference(value, formats, timeZone, DateProcessor.ELASTIC);
                    boolean succeeded = processor.process(doc).isSucceeded();

                    assertThat(succeeded).as("%s %s [%s]", formats, timeZone, value).isEqualTo(expected != null);
                    if (expected != null) {
                        assertThat((String) doc.getField("@timestamp")).as("%s %s [%s]", formats, timeZone, value).isEqualTo(expected);
                    }
                }
            }
        }
    }

    @Test
    public void testFastPathMatchesParseBestOnRandomInput() {
        Random random = new Random(11);
        List<List<String>> formatLists = Arrays.asList(
                Arrays.asList("yyyy-MM-dd HH:mm:ss.SSS"), Arrays.asList("ISO8601"),
                Arrays.asList("yyyy-MM-dd HH:mm:ss.SSS", "ISO8601"), Arrays.asList("ISO8601", "yyyy-MM-dd HH:mm:ss.SSS"));
        List<ZoneId> timeZones = Arrays.asList(null, ZoneId.of("UTC"), ZoneId.of("Europe/Paris"), ZoneId.of("America/New_York"));
        String[] separators = {"T", "T", "T", " ", "t"};
        String[] offsets = {"", "", "Z", "z", "+02:00", "-05:30", "+05:45", "-00:00", "+18:00", "+19:00", "+0200", "+02", "+02:60"};

        for (int n = 0; n < 30000; n++) {
            StringBuilder value = new StringBuilder()
                    .append(String.format("%04d", random.nextInt(10) == 0 ? random.nextInt(3) : 1990 + random.nextInt(50))).append('-')
                    .append(String.format("%02d", random.nextInt(14))).append('-')
                    .append(String.format("%02d", random.nextInt(33)))
                    .append(separators[random.nextInt(separators.length)])
                    .append(String.format("%02d", random.nextInt(26))).append(':')
                    .append(String.format("%02d", random.nextInt(62))).append(':')
                    .append(String.format("%02d", random.nextInt(62)));
            int fraction = random.nextInt(12) - 1;
            if (fraction >= 0) {
                value.append('.');
                for (int d = 0; d < fraction; d++) value.append((char) ('0' + random.nextInt(10)));
            }
            value.append(offsets[random.nextInt(offsets.length)]);

            List<String> formats = formatLists.get(random.nextInt(formatLists.size()));
            ZoneId timeZone = timeZones.get(random.nextInt(timeZones.size()));
            // full precision and the zone id, so any difference in the parsed value shows
            String outputFormat = random.nextBoolean() ? "ELASTIC" : "uuuu-MM-dd'T'HH:mm:ss.SSSSSSSSSxxx'['VV']'";
            DateProcessor processor = new DateProcessor("datetime", "@timestamp", formats, timeZone, outputFormat);
            Doc doc = createDoc("datetime", value.toString());
            DateTimeFormatter output = outputFormat.equals("ELASTIC") ? DateProcessor.ELASTIC : DateTimeFormatter.ofPattern(outputFormat);
            String expected = parseBestReference(value.toString(), formats, timeZone, output);

            assertThat(processor.process(doc).isSucceeded()).as("%s %s [%s]", formats, timeZone, value).isEqualTo(expected != null);
            if (expected != null) {
                assertThat((String) doc.getField("@timestamp")).as("%s %s [%s]", formats, timeZone, value).isEqualTo(expected);
            }
        }
    }

    /** DateProcessor's string parsing before the zone check replaced parseBest. */
    private static String parseBestReference(String value, List<String> formats, ZoneId timeZone, DateTimeFormatter output) {
        for (String format : formats) {
            DateTimeFormatter formatter;
            if (format.toUpperCase().startsWith("UNIX")) {
                formatter = (format.equals("UNIX") ? DateProcessor.UNIX : DateProcessor.UNIX_MS).withZone(timeZone == null ? ZoneId.of("UTC") : timeZone);
            } else if (format.equals("ISO8601")) {
                formatter = DateProcessor.ISO8601;
            } else if (format.equals("ELASTIC")) {
                formatter = DateProcessor.ELASTIC;
            } else {
                formatter = new DateTimeFormatterBuilder().parseCaseInsensitive().appendPattern(format).toFormatter();
            }
            try {
                TemporalAccessor temporal = formatter.parseBest(value, ZonedDateTime::from, LocalDateTime::from);
                ZonedDateTime dateTime = temporal instanceof LocalDateTime
                        ? ZonedDateTime.of((LocalDateTime) temporal, timeZone == null ? ZoneOffset.UTC : timeZone)
                        : ZonedDateTime.from(temporal);
                return dateTime.format(output);
            } catch (DateTimeParseException e) {
                // next format
            }
        }
        return null;
    }
}
