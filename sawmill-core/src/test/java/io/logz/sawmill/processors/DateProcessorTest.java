package io.logz.sawmill.processors;

import com.google.common.collect.ImmutableMap;
import io.logz.sawmill.Doc;
import org.junit.Test;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static io.logz.sawmill.utils.FactoryUtils.createConfig;
import static io.logz.sawmill.utils.FactoryUtils.createProcessor;
import static org.assertj.core.api.Assertions.assertThat;

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
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.elasticPrintFormat));
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
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.elasticPrintFormat));
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
            assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.elasticPrintFormat));
        });
    }

    @Test
    public void testParseInvalidObjects() {
        String field = "datetime";
        String targetField = "@timestamp";
        List<String> formats = Arrays.asList("UNIX_MS");
        ZoneId zoneId = ZoneId.of("UTC");
        Doc docWithMap = createDoc(field, ImmutableMap.of("its", "a", "map", "should", "not", "work"));

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", formats,
                "timeZone", zoneId.toString());

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(docWithMap).isSucceeded()).isFalse();

        Doc docWithList = createDoc(field, Arrays.asList("its", "a", "list", "should", "not", "work"));

        assertThat(dateProcessor.process(docWithList).isSucceeded()).isFalse();
    }

    @Test
    public void testISOFormatWithoutTimezone() {
        String field = "datetime";
        String targetField = "timestamp";

        ZoneId zoneId = ZoneId.of("Europe/Paris");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd/MMM/yyyy:HH:mm:ss Z");
        String dateString = zonedDateTime.format(formatter);
        List<String> formats = Arrays.asList("dd/MMM/yyyy:HH:mm:ss Z");
        Doc doc = createDoc(field, dateString);

        ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", formats);

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(DateProcessor.elasticPrintFormat));
    }

    @Test
    public void testUnixFormatWithoutTimezoneDefaultIsUTC() {
        String field = "datetime";
        String targetField = "@timestamp";
        List<String> formats = Arrays.asList("UNIX");
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        Map<String,Object> config = createConfig("field", field,
                "targetField", targetField,
                "formats", formats);

        DateProcessor dateProcessor = createProcessor(DateProcessor.class, config);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(DateProcessor.elasticPrintFormat));
    }
}
