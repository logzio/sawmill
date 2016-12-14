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

import static io.logz.sawmill.utils.DocUtils.createDoc;
import static java.time.format.DateTimeFormatter.ISO_DATE_TIME;
import static org.assertj.core.api.Assertions.assertThat;

public class DateProcessorTest {
    @Test
    public void testUnixMsFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli());

        DateProcessor dateProcessor = new DateProcessor(field, targetField, Arrays.asList("UNIX_MS"), zoneId);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String)doc.getField(targetField)).isEqualTo(zonedDateTime.format(ISO_DATE_TIME));
    }

    @Test
    public void testUnixFormat() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId).truncatedTo(ChronoUnit.SECONDS);
        Doc doc = createDoc(field, zonedDateTime.toInstant().toEpochMilli() / 1000);

        DateProcessor dateProcessor = new DateProcessor(field, targetField, Arrays.asList("UNIX"), zoneId);

        assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
        assertThat((String) doc.getField(targetField)).isEqualTo(zonedDateTime.format(ISO_DATE_TIME));
    }

    @Test
    public void testSeveralISOPatterns() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        ZonedDateTime zonedDateTime = LocalDateTime.now().atZone(zoneId);
        List<String> formats = Arrays.asList("dd/MM/yyyy HH:mm:ss", "yyyy-MM-dd'T'HH:mm:ssZ", "yyyy-MM-dd HH:mm:ss.SSSSS", "ddMMyyyy HHmmssSSS");

        DateProcessor dateProcessor = new DateProcessor(field, targetField, formats, zoneId);

        formats.forEach(format -> {
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern(format);
            String dateString = zonedDateTime.format(formatter);
            Doc doc = createDoc(field, dateString);

            ZonedDateTime expectedDateTime = LocalDateTime.parse(dateString, formatter).atZone(zoneId);

            assertThat(dateProcessor.process(doc).isSucceeded()).isTrue();
            assertThat((String) doc.getField(targetField)).isEqualTo(expectedDateTime.format(ISO_DATE_TIME));
        });
    }

    @Test
    public void testParseInvalidObjects() {
        String field = "datetime";
        String targetField = "@timestamp";
        ZoneId zoneId = ZoneId.of("UTC");
        Doc docWithMap = createDoc(field, ImmutableMap.of("its", "a", "map", "should", "not", "work"));

        DateProcessor dateProcessor = new DateProcessor(field, targetField, Arrays.asList("UNIX_MS"), zoneId);

        assertThat(dateProcessor.process(docWithMap).isSucceeded()).isFalse();

        Doc docWithList = createDoc(field, Arrays.asList("its", "a", "list", "should", "not", "work"));

        assertThat(dateProcessor.process(docWithList).isSucceeded()).isFalse();
    }
}